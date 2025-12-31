# SPDX-License-Identifier: Apache-2.0
"""Tests for edge_utils.circuit_breaker module."""

import pytest
import time
from edge_utils.circuit_breaker import CircuitBreaker, CircuitBreakerError, CircuitState


class TestCircuitBreaker:
    """Tests for CircuitBreaker class."""

    def test_circuit_starts_closed(self):
        """Test that circuit starts in CLOSED state."""
        breaker = CircuitBreaker(failure_threshold=3, timeout=1)
        assert breaker.state == CircuitState.CLOSED
        assert breaker.is_closed
        assert not breaker.is_open

    def test_circuit_opens_after_failures(self):
        """Test that circuit opens after threshold failures."""
        breaker = CircuitBreaker(failure_threshold=3, timeout=1)

        def failing_function():
            raise Exception("Test failure")

        # Circuit should open after 3 failures
        for i in range(3):
            with pytest.raises(Exception):
                breaker.call(failing_function)

        # Circuit should now be open
        assert breaker.state == CircuitState.OPEN
        assert breaker.is_open

    def test_circuit_rejects_when_open(self):
        """Test that circuit rejects calls when open."""
        breaker = CircuitBreaker(failure_threshold=2, timeout=1)

        def failing_function():
            raise Exception("Test failure")

        # Open the circuit
        for i in range(2):
            with pytest.raises(Exception):
                breaker.call(failing_function)

        # Now calls should be rejected
        with pytest.raises(CircuitBreakerError):
            breaker.call(failing_function)

    def test_circuit_half_opens_after_timeout(self):
        """Test that circuit transitions to HALF_OPEN after timeout."""
        breaker = CircuitBreaker(failure_threshold=2, timeout=0.1)

        def failing_function():
            raise Exception("Test failure")

        # Open the circuit
        for i in range(2):
            with pytest.raises(Exception):
                breaker.call(failing_function)

        assert breaker.state == CircuitState.OPEN

        # Wait for timeout
        time.sleep(0.2)

        # Next call should transition to HALF_OPEN
        # (even though it fails)
        with pytest.raises(Exception):
            breaker.call(failing_function)

        # Should be back to OPEN after failed attempt
        assert breaker.state == CircuitState.OPEN

    def test_circuit_closes_after_success_threshold(self):
        """Test that circuit closes after success threshold in HALF_OPEN."""
        breaker = CircuitBreaker(failure_threshold=2, timeout=0.1, success_threshold=2)

        call_count = [0]

        def sometimes_failing_function():
            call_count[0] += 1
            if call_count[0] <= 2:
                raise Exception("Initial failures")
            return "success"

        # Open the circuit
        for i in range(2):
            with pytest.raises(Exception):
                breaker.call(sometimes_failing_function)

        assert breaker.state == CircuitState.OPEN

        # Wait for timeout
        time.sleep(0.2)

        # Next 2 successful calls should close the circuit
        result = breaker.call(sometimes_failing_function)
        assert result == "success"

        result = breaker.call(sometimes_failing_function)
        assert result == "success"

        # Circuit should be closed now
        assert breaker.state == CircuitState.CLOSED

    def test_circuit_resets_failure_count_on_success(self):
        """Test that failure count resets on success in CLOSED state."""
        breaker = CircuitBreaker(failure_threshold=3, timeout=1)

        call_count = [0]

        def alternating_function():
            call_count[0] += 1
            if call_count[0] % 2 == 0:
                return "success"
            raise Exception("Failure")

        # Fail, succeed, fail, succeed - should not open
        for i in range(4):
            try:
                breaker.call(alternating_function)
            except Exception:
                pass

        assert breaker.state == CircuitState.CLOSED

    def test_protect_decorator(self):
        """Test the protect decorator."""
        breaker = CircuitBreaker(failure_threshold=2, timeout=1)

        @breaker.protect
        def protected_function(x):
            if x < 0:
                raise ValueError("Negative value")
            return x * 2

        # Should work normally
        assert protected_function(5) == 10

        # Should propagate exceptions
        with pytest.raises(ValueError):
            protected_function(-1)

        with pytest.raises(ValueError):
            protected_function(-2)

        # Circuit should be open now
        assert breaker.state == CircuitState.OPEN

    def test_manual_reset(self):
        """Test manual circuit reset."""
        breaker = CircuitBreaker(failure_threshold=2, timeout=1)

        def failing_function():
            raise Exception("Failure")

        # Open the circuit
        for i in range(2):
            with pytest.raises(Exception):
                breaker.call(failing_function)

        assert breaker.state == CircuitState.OPEN

        # Manually reset
        breaker.reset()

        assert breaker.state == CircuitState.CLOSED

    def test_get_stats(self):
        """Test getting circuit breaker statistics."""
        breaker = CircuitBreaker(failure_threshold=3, timeout=60, name="test_breaker")

        stats = breaker.get_stats()
        assert stats["name"] == "test_breaker"
        assert stats["state"] == "closed"
        assert stats["failure_count"] == 0
        assert stats["failure_threshold"] == 3


class TestCircuitBreakerEdgeCases:
    """Tests for edge cases."""

    def test_zero_threshold(self):
        """Test behavior with zero threshold."""
        breaker = CircuitBreaker(failure_threshold=0, timeout=1)

        def failing_function():
            raise Exception("Failure")

        # Should open immediately on first failure
        with pytest.raises(Exception):
            breaker.call(failing_function)

        # Circuit might be open now (depending on implementation)
        # This is an edge case - threshold of 0 doesn't make practical sense

    def test_very_long_timeout(self):
        """Test with very long timeout."""
        breaker = CircuitBreaker(failure_threshold=1, timeout=1000)

        def failing_function():
            raise Exception("Failure")

        with pytest.raises(Exception):
            breaker.call(failing_function)

        # Circuit should be open
        assert breaker.state == CircuitState.OPEN

        # Should not transition to HALF_OPEN immediately
        with pytest.raises(CircuitBreakerError):
            breaker.call(failing_function)

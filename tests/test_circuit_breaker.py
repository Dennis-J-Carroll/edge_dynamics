# SPDX-License-Identifier: Apache-2.0
"""Test circuit breaker functionality."""

import time
import pytest
from edge_dynamics.circuit_breaker import CircuitBreaker, CircuitState

class TestCircuitBreaker:
    """Test circuit breaker functionality."""
    
    def test_initial_state(self):
        """Test that circuit breaker starts in CLOSED state."""
        breaker = CircuitBreaker(name="test")
        
        assert breaker.state == CircuitState.CLOSED
        assert breaker._failure_count == 0
        assert breaker._last_failure_time is None
    
    def test_successful_calls(self):
        """Test that successful calls don't change state."""
        breaker = CircuitBreaker(name="test")
        
        @breaker
        def successful_function():
            return "success"
        
        for i in range(10):
            result = successful_function()
            assert result == "success"
            assert breaker.state == CircuitState.CLOSED
            assert breaker._total_successes == i + 1
    
    def test_circuit_opens_on_failures(self):
        """Test that circuit opens after threshold failures."""
        breaker = CircuitBreaker(
            name="test",
            failure_threshold=3,
            timeout=10.0
        )
        
        call_count = 0
        
        @breaker
        def failing_function():
            nonlocal call_count
            call_count += 1
            if call_count <= 5:
                raise ConnectionError("Simulated failure")
            return "success"
        
        for i in range(3):
            with pytest.raises(ConnectionError):
                failing_function()
        
        assert breaker.state == CircuitState.OPEN
        
        with pytest.raises(Exception, match="Circuit breaker.*is OPEN"):
            failing_function()
    
    def test_circuit_resets_after_timeout(self):
        """Test that circuit attempts recovery after timeout."""
        breaker = CircuitBreaker(
            name="test",
            failure_threshold=2,
            timeout=0.1
        )
        
        @breaker
        def failing_function():
            raise ConnectionError("Failure")
        
        for i in range(2):
            with pytest.raises(ConnectionError):
                failing_function()
        
        assert breaker.state == CircuitState.OPEN
        
        time.sleep(0.15)
        
        assert breaker.state == CircuitState.HALF_OPEN
    
    def test_manual_control(self):
        """Test manual control of circuit breaker."""
        breaker = CircuitBreaker(name="test")
        
        assert breaker.state == CircuitState.CLOSED
        
        breaker.force_open()
        assert breaker.state == CircuitState.OPEN
        
        breaker.force_close()
        assert breaker.state == CircuitState.CLOSED

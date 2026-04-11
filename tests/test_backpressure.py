# SPDX-License-Identifier: Apache-2.0
"""
tests/edge_utils/test_backpressure.py

Tests for BackpressureGate.

Structure:
    TestProportionalDelay        — delay math, no blocking
    TestStrategyDrop             — drop returns False at hard limit
    TestStrategyRaise            — raise fires BackpressureError
    TestStrategyBlock            — block waits, wakes on release
    TestReleaseSemantics         — release decrement, notify, clamp at zero
    TestPressureMetric           — normalized pressure property
    TestStats                    — counters increment correctly
    TestConcurrency              — producer flood vs consumer flush race
    TestEdgeCases                — limits, n=0, invalid config
    TestIntegrationWithAgent     — simulated enqueue/flush loop
"""

import threading
import time

import pytest

from edge_dynamics.edge_utils.backpressure import (
    BackpressureGate,
    BackpressureError,
    BackpressureTimeout,
)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def make_gate(**kwargs) -> BackpressureGate:
    """Sensible defaults for test gates."""
    defaults = dict(
        soft_limit=10,
        hard_limit=20,
        strategy="drop",
        max_delay_ms=50.0,
        block_timeout_ms=500.0,
    )
    defaults.update(kwargs)
    return BackpressureGate(**defaults)


# ──────────────────────────────────────────────────────────────────────────────
# TestProportionalDelay
# ──────────────────────────────────────────────────────────────────────────────

class TestProportionalDelay:
    """Test the delay ramp math without any blocking."""

    def test_no_delay_below_soft_limit(self):
        gate = make_gate(soft_limit=10, hard_limit=20, max_delay_ms=100)
        # Below soft limit → delay = 0
        delay = gate._proportional_delay_ms(queued=5)
        assert delay == 0.0

    def test_no_delay_exactly_at_soft_limit(self):
        gate = make_gate(soft_limit=10, hard_limit=20, max_delay_ms=100)
        delay = gate._proportional_delay_ms(queued=10)
        assert delay == 0.0

    def test_half_delay_at_midpoint(self):
        """At midpoint between soft and hard, delay should be 50% of max."""
        gate = make_gate(soft_limit=10, hard_limit=30, max_delay_ms=100)
        delay = gate._proportional_delay_ms(queued=20)  # midpoint
        assert delay == pytest.approx(50.0)

    def test_full_delay_at_hard_limit(self):
        gate = make_gate(soft_limit=10, hard_limit=20, max_delay_ms=100)
        delay = gate._proportional_delay_ms(queued=20)
        assert delay == pytest.approx(100.0)

    def test_delay_capped_at_max_delay(self):
        """Queued beyond hard_limit shouldn't exceed max_delay_ms."""
        gate = make_gate(soft_limit=10, hard_limit=20, max_delay_ms=100)
        delay = gate._proportional_delay_ms(queued=999)
        assert delay == pytest.approx(100.0)

    def test_delay_ramp_is_linear(self):
        """Verify linearity: each step in the soft zone adds equal delay."""
        gate = make_gate(soft_limit=0, hard_limit=100, max_delay_ms=100)
        delays = [gate._proportional_delay_ms(q) for q in range(0, 101, 10)]
        diffs = [delays[i+1] - delays[i] for i in range(len(delays)-1)]
        # All diffs should be equal (linear ramp)
        assert all(abs(d - diffs[0]) < 0.01 for d in diffs)

    def test_delay_formula_is_difference_based(self):
        """
        Explicitly verify: delay = max * (queued - soft) / (hard - soft)
        Both numerator and denominator are differences.
        """
        soft, hard, mx = 10, 50, 200
        gate = make_gate(soft_limit=soft, hard_limit=hard, max_delay_ms=mx)

        for q in [15, 20, 30, 40, 50]:
            expected = mx * (q - soft) / (hard - soft)
            actual = gate._proportional_delay_ms(q)
            assert actual == pytest.approx(expected, rel=1e-6)


# ──────────────────────────────────────────────────────────────────────────────
# TestStrategyDrop
# ──────────────────────────────────────────────────────────────────────────────

class TestStrategyDrop:
    """Acquire returns False at hard limit — never blocks."""

    def test_acquire_succeeds_below_hard_limit(self):
        gate = make_gate(strategy="drop", soft_limit=10, hard_limit=20)
        for _ in range(15):
            assert gate.acquire(1) is True
        assert gate.queued == 15

    def test_acquire_returns_false_at_hard_limit(self):
        gate = make_gate(strategy="drop", soft_limit=5, hard_limit=10)
        for _ in range(10):
            gate.acquire(1)
        assert gate.queued == 10

        # One more → hard limit → drop
        result = gate.acquire(1)
        assert result is False
        assert gate.queued == 10  # unchanged

    def test_drop_increments_dropped_counter(self):
        gate = make_gate(strategy="drop", soft_limit=1, hard_limit=2,
                         max_delay_ms=0)
        gate.acquire(1)
        gate.acquire(1)   # fills to hard limit
        gate.acquire(1)   # dropped
        gate.acquire(1)   # dropped
        stats = gate.get_stats()
        assert stats["total_dropped"] == 2

    def test_acquire_succeeds_after_release(self):
        gate = make_gate(strategy="drop", soft_limit=1, hard_limit=3,
                         max_delay_ms=0)
        gate.acquire(1)
        gate.acquire(1)
        gate.acquire(1)
        assert gate.queued == 3

        # At limit — next acquire fails
        assert gate.acquire(1) is False

        # Release one → now there's space
        gate.release(1)
        assert gate.queued == 2
        assert gate.acquire(1) is True

    def test_acquire_n_greater_than_one(self):
        gate = make_gate(strategy="drop", soft_limit=5, hard_limit=10,
                         max_delay_ms=0)
        assert gate.acquire(8) is True
        assert gate.queued == 8
        # Acquiring 3 more would exceed limit (8+3=11 > 10)
        assert gate.acquire(3) is False
        assert gate.queued == 8  # unchanged


# ──────────────────────────────────────────────────────────────────────────────
# TestStrategyRaise
# ──────────────────────────────────────────────────────────────────────────────

class TestStrategyRaise:
    def test_raises_at_hard_limit(self):
        gate = make_gate(strategy="raise", soft_limit=1, hard_limit=3,
                         max_delay_ms=0)
        gate.acquire(3)
        with pytest.raises(BackpressureError) as exc_info:
            gate.acquire(1)
        assert exc_info.value.hard_limit == 3
        assert exc_info.value.queued == 3

    def test_does_not_raise_below_hard_limit(self):
        gate = make_gate(strategy="raise", soft_limit=1, hard_limit=5,
                         max_delay_ms=0)
        for _ in range(5):
            gate.acquire(1)   # should not raise
        assert gate.queued == 5

    def test_exception_carries_useful_info(self):
        gate = make_gate(strategy="raise", soft_limit=1, hard_limit=2,
                         max_delay_ms=0)
        gate.acquire(2)
        with pytest.raises(BackpressureError) as exc_info:
            gate.acquire(1)
        err = exc_info.value
        assert err.queued == 2
        assert err.hard_limit == 2


# ──────────────────────────────────────────────────────────────────────────────
# TestStrategyBlock
# ──────────────────────────────────────────────────────────────────────────────

class TestStrategyBlock:
    def test_acquire_blocks_until_release(self):
        """
        Producer fills to hard limit, then blocks.
        Consumer releases after 100ms.
        Producer should unblock and succeed.
        """
        gate = make_gate(strategy="block", soft_limit=2, hard_limit=5,
                         max_delay_ms=0, block_timeout_ms=2000)

        gate.acquire(5)  # fill to hard limit
        assert gate.queued == 5

        released_at = [0.0]
        unblocked_at = [0.0]

        def consumer():
            time.sleep(0.1)
            released_at[0] = time.monotonic()
            gate.release(3)   # make space

        def producer():
            gate.acquire(1)   # should block ~100ms then succeed
            unblocked_at[0] = time.monotonic()

        c = threading.Thread(target=consumer)
        p = threading.Thread(target=producer)
        p.start()
        c.start()
        p.join(timeout=2.0)
        c.join()

        assert not p.is_alive(), "Producer thread should have unblocked"
        # Producer unblocked AFTER consumer released
        assert unblocked_at[0] >= released_at[0]

    def test_block_timeout_raises_backpressure_timeout(self):
        gate = make_gate(strategy="block", soft_limit=1, hard_limit=2,
                         max_delay_ms=0, block_timeout_ms=100)
        gate.acquire(2)  # fill to hard limit

        with pytest.raises(BackpressureTimeout):
            gate.acquire(1)   # will wait 100ms then raise

    def test_multiple_blocked_producers_all_wake(self):
        """
        Three producers block. One large release wakes them all.
        """
        gate = make_gate(strategy="block", soft_limit=1, hard_limit=3,
                         max_delay_ms=0, block_timeout_ms=2000)
        gate.acquire(3)  # fill

        results = []
        lock = threading.Lock()

        def blocked_producer():
            result = gate.acquire(1)
            with lock:
                results.append(result)

        # Start 3 blocked producers
        threads = [threading.Thread(target=blocked_producer) for _ in range(3)]
        for t in threads:
            t.start()

        time.sleep(0.05)  # let them block

        # Release 3 → all should wake
        gate.release(3)

        for t in threads:
            t.join(timeout=2.0)

        assert len(results) == 3
        assert all(r is True for r in results)


# ──────────────────────────────────────────────────────────────────────────────
# TestReleaseSemantics
# ──────────────────────────────────────────────────────────────────────────────

class TestReleaseSemantics:
    def test_release_decrements_queued(self):
        gate = make_gate(strategy="drop", max_delay_ms=0)
        gate.acquire(5)
        assert gate.queued == 5
        gate.release(3)
        assert gate.queued == 2

    def test_release_clamps_at_zero(self):
        """Releasing more than queued should not go negative."""
        gate = make_gate(strategy="drop", max_delay_ms=0)
        gate.acquire(3)
        gate.release(10)   # release more than acquired
        assert gate.queued == 0

    def test_release_zero_is_noop(self):
        gate = make_gate(strategy="drop", max_delay_ms=0)
        gate.acquire(3)
        gate.release(0)
        assert gate.queued == 3

    def test_release_increments_stats(self):
        gate = make_gate(strategy="drop", max_delay_ms=0)
        gate.acquire(5)
        gate.release(3)
        stats = gate.get_stats()
        assert stats["total_released"] == 3


# ──────────────────────────────────────────────────────────────────────────────
# TestPressureMetric
# ──────────────────────────────────────────────────────────────────────────────

class TestPressureMetric:
    """The normalized pressure property: (queued - soft) / (hard - soft)."""

    def test_zero_pressure_below_soft(self):
        gate = make_gate(soft_limit=10, hard_limit=20, max_delay_ms=0)
        gate.acquire(5)
        assert gate.pressure == 0.0

    def test_zero_pressure_at_soft(self):
        gate = make_gate(soft_limit=10, hard_limit=20, max_delay_ms=0,
                         strategy="drop")
        gate.acquire(10)
        assert gate.pressure == 0.0

    def test_half_pressure_at_midpoint(self):
        gate = make_gate(soft_limit=0, hard_limit=20, max_delay_ms=0,
                         strategy="drop")
        gate.acquire(10)
        assert gate.pressure == pytest.approx(0.5)

    def test_full_pressure_at_hard_limit(self):
        gate = make_gate(soft_limit=5, hard_limit=10, max_delay_ms=0,
                         strategy="drop")
        gate.acquire(10)
        assert gate.pressure == pytest.approx(1.0)

    def test_pressure_decreases_on_release(self):
        gate = make_gate(soft_limit=0, hard_limit=10, max_delay_ms=0,
                         strategy="drop")
        gate.acquire(10)
        assert gate.pressure == pytest.approx(1.0)
        gate.release(5)
        assert gate.pressure == pytest.approx(0.5)


# ──────────────────────────────────────────────────────────────────────────────
# TestStats
# ──────────────────────────────────────────────────────────────────────────────

class TestStats:
    def test_acquired_counter(self):
        gate = make_gate(strategy="drop", max_delay_ms=0)
        gate.acquire(3)
        gate.acquire(2)
        stats = gate.get_stats()
        assert stats["total_acquired"] == 5

    def test_peak_queued_tracks_high_water_mark(self):
        gate = make_gate(soft_limit=100, hard_limit=200, strategy="drop",
                         max_delay_ms=0)
        gate.acquire(50)
        gate.acquire(50)   # peak: 100
        gate.release(80)   # drops to 20
        gate.acquire(10)   # now 30
        stats = gate.get_stats()
        assert stats["peak_queued"] == 100

    def test_stats_includes_config(self):
        gate = make_gate(soft_limit=42, hard_limit=99, strategy="raise",
                         max_delay_ms=0)
        stats = gate.get_stats()
        assert stats["soft_limit"] == 42
        assert stats["hard_limit"] == 99
        assert stats["strategy"] == "raise"

    def test_queue_pct_in_stats(self):
        gate = make_gate(soft_limit=5, hard_limit=10, strategy="drop",
                         max_delay_ms=0)
        gate.acquire(5)
        stats = gate.get_stats()
        assert stats["queue_pct"] == pytest.approx(50.0)


# ──────────────────────────────────────────────────────────────────────────────
# TestConcurrency
# ──────────────────────────────────────────────────────────────────────────────

class TestConcurrency:
    """Race condition and thread safety tests."""

    def test_total_acquired_is_exact_under_concurrent_acquires(self):
        """
        50 threads, each acquiring 10 messages.
        Total acquired + total dropped must equal 50*10 = 500.
        """
        gate = make_gate(
            soft_limit=100,
            hard_limit=200,
            strategy="drop",
            max_delay_ms=0,
        )
        acquired_count = [0]
        dropped_count = [0]
        lock = threading.Lock()

        def producer():
            for _ in range(10):
                if gate.acquire(1):
                    with lock:
                        acquired_count[0] += 1
                else:
                    with lock:
                        dropped_count[0] += 1

        threads = [threading.Thread(target=producer) for _ in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        total = acquired_count[0] + dropped_count[0]
        assert total == 500
        # Queue depth should match acquired
        assert gate.queued == acquired_count[0]
        # Stats should match
        stats = gate.get_stats()
        assert stats["total_acquired"] == acquired_count[0]
        assert stats["total_dropped"] == dropped_count[0]

    def test_producer_consumer_throughput(self):
        """
        Producer floods at max speed.
        Consumer releases in batches every 10ms.
        Queue should stay bounded below hard_limit.
        """
        gate = make_gate(
            soft_limit=50,
            hard_limit=100,
            strategy="drop",
            max_delay_ms=20.0,
        )
        stop = threading.Event()
        peak_seen = [0]

        def consumer():
            while not stop.is_set():
                time.sleep(0.01)
                gate.release(20)

        def producer():
            while not stop.is_set():
                gate.acquire(1)
                q = gate.queued
                if q > peak_seen[0]:
                    peak_seen[0] = q

        c = threading.Thread(target=consumer)
        p1 = threading.Thread(target=producer)
        p2 = threading.Thread(target=producer)

        c.start(); p1.start(); p2.start()
        time.sleep(0.3)
        stop.set()
        c.join(); p1.join(); p2.join()

        # Queue should never have exceeded hard_limit
        assert peak_seen[0] <= gate.hard_limit

    def test_no_deadlock_with_block_strategy(self):
        """
        Block strategy: producer blocks, consumer releases, producer unblocks.
        Run 100 times to shake out any deadlock.
        """
        for _ in range(100):
            gate = make_gate(
                strategy="block",
                soft_limit=1,
                hard_limit=2,
                max_delay_ms=0,
                block_timeout_ms=1000,
            )
            gate.acquire(2)  # fill

            def release_soon():
                time.sleep(0.005)
                gate.release(2)

            t = threading.Thread(target=release_soon)
            t.start()
            gate.acquire(1)  # should unblock
            t.join()


# ──────────────────────────────────────────────────────────────────────────────
# TestEdgeCases
# ──────────────────────────────────────────────────────────────────────────────

class TestEdgeCases:
    def test_invalid_config_soft_gte_hard(self):
        with pytest.raises(ValueError, match="soft_limit"):
            BackpressureGate(soft_limit=10, hard_limit=10)

    def test_invalid_config_soft_greater_than_hard(self):
        with pytest.raises(ValueError):
            BackpressureGate(soft_limit=20, hard_limit=10)

    def test_acquire_zero_is_always_true(self):
        gate = make_gate(strategy="drop", max_delay_ms=0)
        gate.acquire(gate.hard_limit)   # fill completely
        assert gate.acquire(0) is True   # n=0 always succeeds

    def test_release_on_empty_gate_is_safe(self):
        gate = make_gate()
        gate.release(100)   # nothing to release
        assert gate.queued == 0

    def test_context_manager_releases_on_success(self):
        gate = make_gate(strategy="drop", soft_limit=5, hard_limit=10,
                         max_delay_ms=0)
        with gate.managed(n=3) as ok:
            assert ok is True
            assert gate.queued == 3
        # After context: released
        assert gate.queued == 0

    def test_context_manager_releases_on_exception(self):
        gate = make_gate(strategy="drop", soft_limit=5, hard_limit=10,
                         max_delay_ms=0)
        try:
            with gate.managed(n=3):
                assert gate.queued == 3
                raise RuntimeError("boom")
        except RuntimeError:
            pass
        assert gate.queued == 0

    def test_context_manager_does_not_release_if_dropped(self):
        gate = make_gate(strategy="drop", soft_limit=1, hard_limit=2,
                         max_delay_ms=0)
        gate.acquire(2)   # fill to limit

        with gate.managed(n=1) as ok:
            assert ok is False
            assert gate.queued == 2  # unchanged
        # Still unchanged after context (nothing acquired, nothing to release)
        assert gate.queued == 2


# ──────────────────────────────────────────────────────────────────────────────
# TestIntegrationWithAgent
# ──────────────────────────────────────────────────────────────────────────────

class TestIntegrationWithAgent:
    """
    Simulates the actual agent integration pattern:
        enqueue() → acquire(1)
        _flush_batch() → release(batch_size)

    Verifies: queue depth stays bounded, no data corruption,
    stats are consistent.
    """

    def test_simulated_agent_loop_stays_bounded(self):
        """
        Simulate 2000 messages from a fast producer with a slow flush loop.
        The gate should keep in-flight count below hard_limit.
        """
        gate = BackpressureGate(
            soft_limit=50,
            hard_limit=100,
            strategy="drop",
            max_delay_ms=5.0,
        )
        stop = threading.Event()
        in_flight = []   # record queue depth over time

        # Flush loop: drains batches of 10 every 20ms
        def flush_loop():
            while not stop.is_set():
                time.sleep(0.02)
                batch_size = min(gate.queued, 10)
                if batch_size > 0:
                    gate.release(batch_size)

        flush = threading.Thread(target=flush_loop)
        flush.start()

        # Producer: send 2000 messages as fast as possible
        acquired = 0
        dropped = 0
        for _ in range(2000):
            ok = gate.acquire(1)
            in_flight.append(gate.queued)
            if ok:
                acquired += 1
            else:
                dropped += 1

        stop.set()
        flush.join()

        # Invariant: queue depth never exceeded hard_limit
        assert max(in_flight) <= gate.hard_limit

        # Invariant: acquired + dropped == total attempted
        assert acquired + dropped == 2000

        # Invariant: stats are consistent
        stats = gate.get_stats()
        assert stats["total_acquired"] == acquired
        assert stats["total_dropped"] == dropped

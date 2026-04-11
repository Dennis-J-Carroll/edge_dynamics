# SPDX-License-Identifier: Apache-2.0
"""
edge_utils/backpressure.py

BackpressureGate: feedback signal from the agent back to the producer.

The problem it solves:
    Without backpressure, a fast producer + sustained outage =
    unbounded in-memory deque growth → OOM → process crash →
    loss of DLM state, sample buffers, EMA baselines.
    The "zero-loss" claim breaks — not from the network, from RAM.

The solution:
    Track how many messages are currently in-flight (enqueued but
    not yet flushed). When that count climbs:

        Below soft_limit   →  no action, free pass
        soft → hard        →  proportional delay (producer slows smoothly)
        At hard_limit      →  configured strategy: block | drop | raise

The proportional delay is a linear ramp:

    delay_ms = max_delay_ms * (queued - soft_limit) / (hard_limit - soft_limit)

This is continuous feedback — the producer feels resistance before hitting
the wall. It mirrors how TCP congestion control works: slow down before
you drop, not after.

─────────────────────────────────────────────────────────────────────
Connection to the Minus One Principle
─────────────────────────────────────────────────────────────────────
The delay ramp computes:

    pressure = (queued - soft_limit) / (hard_limit - soft_limit)
             = (queued - soft_limit) / (hard_limit - soft_limit)
                ↑ subtraction ↑                ↑ subtraction ↑

Both numerator and denominator are differences.
Pressure IS a normalized difference signal — exactly like how attention
scores are softmax(QKᵀ / √d) where QKᵀ is a dot product of differences.
The gate exerts force proportional to how far the system has deviated
from its target (soft_limit). Correction via subtraction, again.
─────────────────────────────────────────────────────────────────────

Usage:
    gate = BackpressureGate(soft_limit=1000, hard_limit=5000)

    # Producer side — in enqueue():
    if not gate.acquire(n=1):
        # dropped (strategy="drop") — log and continue
        return

    # Consumer side — in _flush_batch(), before any processing:
    gate.release(n=len(msgs))

    # Or use context manager (for callers that own both sides):
    with gate.managed(n=1) as ok:
        if ok:
            do_work()
"""

import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Generator, Literal, Optional

try:
    from .logging import get_logger
    _log = get_logger("backpressure")
    def _info(msg, **kw):  _log.info(msg, **kw)
    def _warn(msg, **kw):  _log.warning(msg, **kw)
    def _debug(msg, **kw): _log.debug(msg, **kw)
except ImportError:
    def _info(msg, **kw):  print(f"[INFO]  {msg}", kw or "")
    def _warn(msg, **kw):  print(f"[WARN]  {msg}", kw or "")
    def _debug(msg, **kw): pass


# ──────────────────────────────────────────────────────────────────────────────
# Exceptions
# ──────────────────────────────────────────────────────────────────────────────

class BackpressureError(Exception):
    """
    Raised when strategy="raise" and the hard limit is reached.
    Callers that want explicit control over overload handling should
    catch this and implement their own shedding logic.
    """
    def __init__(self, queued: int, hard_limit: int):
        self.queued = queued
        self.hard_limit = hard_limit
        super().__init__(
            f"BackpressureGate hard limit reached: {queued}/{hard_limit} messages queued"
        )


class BackpressureTimeout(BackpressureError):
    """
    Raised when strategy="block" and block_timeout_ms is exceeded.
    Indicates the downstream flush loop is critically behind.
    """
    def __init__(self, queued: int, hard_limit: int, waited_ms: float):
        self.waited_ms = waited_ms
        super().__init__(queued, hard_limit)
        self.args = (
            f"BackpressureGate block timeout after {waited_ms:.0f}ms: "
            f"{queued}/{hard_limit} messages still queued",
        )


# ──────────────────────────────────────────────────────────────────────────────
# Stats
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class BackpressureStats:
    """Cumulative statistics. All fields are monotonically increasing."""
    total_acquired: int = 0       # messages that successfully entered the queue
    total_released: int = 0       # messages that left the queue (flushed/buffered)
    total_delayed: int = 0        # acquire() calls that triggered a soft delay
    total_delay_ms: float = 0.0   # cumulative milliseconds spent in soft delay
    total_dropped: int = 0        # messages dropped (strategy="drop")
    total_blocked: int = 0        # acquire() calls that entered block-wait
    total_timeouts: int = 0       # block-wait calls that timed out
    peak_queued: int = 0          # high-water mark of concurrent in-flight msgs
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "total_acquired": self.total_acquired,
                "total_released": self.total_released,
                "total_delayed": self.total_delayed,
                "total_delay_ms": round(self.total_delay_ms, 2),
                "total_dropped": self.total_dropped,
                "total_blocked": self.total_blocked,
                "total_timeouts": self.total_timeouts,
                "peak_queued": self.peak_queued,
            }


# ──────────────────────────────────────────────────────────────────────────────
# BackpressureGate
# ──────────────────────────────────────────────────────────────────────────────

class BackpressureGate:
    """
    Two-threshold feedback gate with configurable overload strategy.

    Thresholds:
        soft_limit  — proportional delay begins here (producer slows)
        hard_limit  — ceiling; strategy determines behavior at this point

    Strategies:
        "block"   — wait until space opens (good for reliable producers
                    where data loss is unacceptable; pairs with disk buffer)
        "drop"    — return False immediately; caller drops the message
                    (good for non-critical metrics / high-rate sensors)
        "raise"   — raise BackpressureError; caller decides what to do

    Thread safety:
        Fully thread-safe. Multiple producer threads can call acquire()
        concurrently. The Condition variable ensures block-mode waiters
        are woken exactly when space opens — no spin-waiting.

    Args:
        soft_limit:        Messages in flight before throttling starts.
        hard_limit:        Absolute ceiling. Must be > soft_limit.
        strategy:          Overload behavior: "block" | "drop" | "raise".
        max_delay_ms:      Maximum per-acquire delay in soft zone (default 100ms).
        block_timeout_ms:  Max time to wait in "block" mode before raising
                           BackpressureTimeout (default 5000ms).
    """

    def __init__(
        self,
        soft_limit: int = 1_000,
        hard_limit: int = 5_000,
        strategy: Literal["block", "drop", "raise"] = "block",
        max_delay_ms: float = 100.0,
        block_timeout_ms: float = 5_000.0,
    ) -> None:
        if soft_limit >= hard_limit:
            raise ValueError(
                f"soft_limit ({soft_limit}) must be < hard_limit ({hard_limit})"
            )
        if soft_limit < 0 or hard_limit < 1:
            raise ValueError("Limits must be positive")

        self.soft_limit = soft_limit
        self.hard_limit = hard_limit
        self.strategy = strategy
        self.max_delay_ms = max_delay_ms
        self.block_timeout_ms = block_timeout_ms

        self._queued: int = 0
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._stats = BackpressureStats()

        _info("backpressure_gate_initialized",
              soft_limit=soft_limit,
              hard_limit=hard_limit,
              strategy=strategy,
              max_delay_ms=max_delay_ms)

    # ── Core API ──────────────────────────────────────────────────────────────

    def acquire(self, n: int = 1) -> bool:
        """
        Request n slots in the gate (called before adding messages to queue).

        Returns:
            True  — slots acquired, message(s) can be enqueued.
            False — slots NOT acquired (strategy="drop" only).

        Raises:
            BackpressureError   — strategy="raise" and at hard limit.
            BackpressureTimeout — strategy="block" and timed out waiting.

        Design note: the proportional delay is computed and applied
        BEFORE acquiring the lock. This prevents the soft-zone delay
        from blocking concurrent release() calls — which would be a
        deadlock if the flush thread needs the lock to release slots.
        """
        if n <= 0:
            return True

        # ── Step 1: Compute soft-zone delay (lock-free read is intentional) ──
        # We do a non-atomic read here. It's fine: the delay is approximate
        # feedback, not a hard contract. The hard decisions happen under lock.
        current = self._queued
        delay_ms = self._proportional_delay_ms(current)

        if delay_ms > 0:
            t0 = time.perf_counter()
            time.sleep(delay_ms / 1000.0)
            actual_ms = (time.perf_counter() - t0) * 1000

            with self._stats._lock:
                self._stats.total_delayed += 1
                self._stats.total_delay_ms += actual_ms

            _debug("soft_backpressure_delay",
                   queued=current,
                   soft_limit=self.soft_limit,
                   delay_ms=round(actual_ms, 1))

        # ── Step 2: Hard limit check under lock ───────────────────────────────
        if self.strategy == "block":
            return self._acquire_blocking(n)
        elif self.strategy == "drop":
            return self._acquire_drop(n)
        elif self.strategy == "raise":
            return self._acquire_raise(n)
        else:
            raise ValueError(f"Unknown strategy: {self.strategy!r}")

    def release(self, n: int = 1) -> None:
        """
        Return n slots to the gate (called when messages leave the queue).

        Call this at the START of _flush_batch(), before the actual send,
        because the messages have already been popped from the deque at
        that point — they're no longer consuming queue capacity.

        Notifies all blocked acquire() waiters so they can re-check.
        """
        if n <= 0:
            return

        with self._condition:
            self._queued = max(0, self._queued - n)
            with self._stats._lock:
                self._stats.total_released += n
            self._condition.notify_all()  # wake blocked acquirers

        _debug("backpressure_released",
               released=n,
               remaining=self._queued)

    @contextmanager
    def managed(self, n: int = 1) -> Generator[bool, None, None]:
        """
        Context manager that acquires on enter and releases on exit.

        Usage:
            with gate.managed(n=1) as acquired:
                if acquired:
                    process_message(msg)

        The release only happens if acquire returned True.
        """
        acquired = self.acquire(n)
        try:
            yield acquired
        finally:
            if acquired:
                self.release(n)

    # ── Status ────────────────────────────────────────────────────────────────

    @property
    def queued(self) -> int:
        """Current number of messages in-flight (lock-free approximate read)."""
        return self._queued

    @property
    def pressure(self) -> float:
        """
        Normalized pressure ∈ [0.0, 1.0].

        0.0 = below soft limit (no pressure)
        0.5 = halfway between soft and hard limits
        1.0 = at or above hard limit (critical)

        This is the normalized difference:
            pressure = (queued - soft_limit) / (hard_limit - soft_limit)
        clamped to [0, 1]. Another difference-based signal.
        """
        q = self._queued
        if q <= self.soft_limit:
            return 0.0
        return min(1.0, (q - self.soft_limit) / (self.hard_limit - self.soft_limit))

    def get_stats(self) -> dict:
        """Full statistics snapshot for health endpoints and logging."""
        with self._lock:
            current = self._queued
            pct = current / self.hard_limit * 100

        stats = self._stats.snapshot()
        stats.update({
            "current_queued": current,
            "soft_limit": self.soft_limit,
            "hard_limit": self.hard_limit,
            "pressure": round(self.pressure, 4),
            "queue_pct": round(pct, 1),
            "strategy": self.strategy,
        })
        return stats

    # ── Strategy implementations ──────────────────────────────────────────────

    def _acquire_blocking(self, n: int) -> bool:
        """
        Block until n slots are available or timeout expires.
        Uses a Condition variable — no spin-waiting.
        """
        deadline = time.monotonic() + self.block_timeout_ms / 1000.0

        with self._condition:
            if self._queued + n > self.hard_limit:
                # Need to wait
                with self._stats._lock:
                    self._stats.total_blocked += 1

                _warn("backpressure_blocking",
                      queued=self._queued,
                      hard_limit=self.hard_limit,
                      waiting_for=n)

                while self._queued + n > self.hard_limit:
                    remaining_s = deadline - time.monotonic()
                    if remaining_s <= 0:
                        waited_ms = self.block_timeout_ms
                        with self._stats._lock:
                            self._stats.total_timeouts += 1
                        _warn("backpressure_timeout",
                              queued=self._queued,
                              waited_ms=round(waited_ms, 1))
                        raise BackpressureTimeout(self._queued, self.hard_limit, waited_ms)

                    # Wait with a short timeout so we can recheck deadline
                    self._condition.wait(timeout=min(0.05, remaining_s))

            # Slot available — acquire
            self._queued += n
            self._update_peak()

        with self._stats._lock:
            self._stats.total_acquired += n

        return True

    def _acquire_drop(self, n: int) -> bool:
        """
        Drop immediately if at hard limit. Never blocks.
        Returns False to signal the caller should discard the message(s).
        """
        with self._condition:
            if self._queued + n > self.hard_limit:
                with self._stats._lock:
                    self._stats.total_dropped += n
                _warn("backpressure_drop",
                      queued=self._queued,
                      hard_limit=self.hard_limit,
                      dropped=n)
                return False

            self._queued += n
            self._update_peak()

        with self._stats._lock:
            self._stats.total_acquired += n

        return True

    def _acquire_raise(self, n: int) -> bool:
        """
        Raise BackpressureError immediately if at hard limit.
        """
        with self._condition:
            if self._queued + n > self.hard_limit:
                raise BackpressureError(self._queued, self.hard_limit)

            self._queued += n
            self._update_peak()

        with self._stats._lock:
            self._stats.total_acquired += n

        return True

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _proportional_delay_ms(self, queued: int) -> float:
        """
        Linear ramp from 0ms at soft_limit to max_delay_ms at hard_limit.

        delay = max_delay_ms * (queued - soft_limit) / (hard_limit - soft_limit)

        Numerator:   queued - soft_limit   ← difference
        Denominator: hard_limit - soft_limit ← difference
        Pressure is a ratio of two differences.
        """
        if queued <= self.soft_limit:
            return 0.0
        excess = queued - self.soft_limit
        span = max(self.hard_limit - self.soft_limit, 1)
        return min(self.max_delay_ms, self.max_delay_ms * excess / span)

    def _update_peak(self) -> None:
        """Update peak_queued. Must be called under self._lock / self._condition."""
        if self._queued > self._stats.peak_queued:
            with self._stats._lock:
                if self._queued > self._stats.peak_queued:
                    self._stats.peak_queued = self._queued

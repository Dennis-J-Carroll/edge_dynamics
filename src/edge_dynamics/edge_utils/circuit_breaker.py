# SPDX-License-Identifier: Apache-2.0
"""
Circuit breaker pattern implementation for edge_dynamics.

Prevents cascading failures by detecting when a remote service is down
and temporarily stopping requests to it, allowing it time to recover.

Example:
    >>> from edge_utils.circuit_breaker import CircuitBreaker
    >>> breaker = CircuitBreaker(failure_threshold=5, timeout=60)
    >>>
    >>> @breaker.protect
    >>> def send_to_collector(data):
    ...     return socket.send(data)
"""

import threading
import time
from enum import Enum
from functools import wraps
from typing import Callable, Optional, TypeVar, Any


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if service recovered


class CircuitBreakerError(Exception):
    """Exception raised when circuit is open."""

    pass


T = TypeVar("T")


class CircuitBreaker:
    """
    Circuit breaker to prevent cascading failures.

    The circuit breaker monitors for failures and transitions between states:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Too many failures, requests are rejected immediately
    - HALF_OPEN: Testing if service has recovered

    Attributes:
        failure_threshold: Number of failures before opening circuit
        timeout: Seconds to wait before trying again after opening
        success_threshold: Successes needed in HALF_OPEN to close circuit
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        timeout: float = 60.0,
        success_threshold: int = 2,
        name: str = "circuit_breaker",
    ):
        """
        Initialize circuit breaker.

        Args:
            failure_threshold: Consecutive failures before opening (default: 5)
            timeout: Seconds before attempting recovery (default: 60)
            success_threshold: Consecutive successes to close in HALF_OPEN (default: 2)
            name: Name for logging/identification

        Example:
            >>> breaker = CircuitBreaker(failure_threshold=3, timeout=30)
        """
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.success_threshold = success_threshold
        self.name = name

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._lock = threading.Lock()

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        with self._lock:
            return self._state

    @property
    def is_open(self) -> bool:
        """Check if circuit is open."""
        return self.state == CircuitState.OPEN

    @property
    def is_closed(self) -> bool:
        """Check if circuit is closed."""
        return self.state == CircuitState.CLOSED

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset."""
        if self._last_failure_time is None:
            return False
        return (time.time() - self._last_failure_time) >= self.timeout

    def _record_success(self) -> None:
        """Record a successful call."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.success_threshold:
                    # Recovered! Close the circuit
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._success_count = 0
            elif self._state == CircuitState.CLOSED:
                # Reset failure count on success
                self._failure_count = 0

    def _record_failure(self) -> None:
        """Record a failed call."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                # Failed during recovery, go back to OPEN
                self._state = CircuitState.OPEN
                self._success_count = 0
            elif self._state == CircuitState.CLOSED:
                # Check if we've exceeded threshold
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitState.OPEN

    def call(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """
        Execute function with circuit breaker protection.

        Args:
            func: Function to call
            *args: Positional arguments for func
            **kwargs: Keyword arguments for func

        Returns:
            Result of func(*args, **kwargs)

        Raises:
            CircuitBreakerError: If circuit is open

        Example:
            >>> breaker = CircuitBreaker()
            >>> result = breaker.call(send_data, data)
        """
        with self._lock:
            if self._state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    # Try recovery
                    self._state = CircuitState.HALF_OPEN
                    self._success_count = 0
                else:
                    # Still too soon
                    raise CircuitBreakerError(
                        f"Circuit breaker '{self.name}' is OPEN. "
                        f"Wait {self.timeout - (time.time() - self._last_failure_time):.1f}s"
                    )

        # Try the call
        try:
            result = func(*args, **kwargs)
            self._record_success()
            return result
        except Exception as e:
            self._record_failure()
            raise e

    def protect(self, func: Callable[..., T]) -> Callable[..., T]:
        """
        Decorator to protect a function with circuit breaker.

        Args:
            func: Function to protect

        Returns:
            Wrapped function

        Example:
            >>> breaker = CircuitBreaker()
            >>>
            >>> @breaker.protect
            >>> def send_data(data):
            ...     return socket.send(data)
        """

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            return self.call(func, *args, **kwargs)

        return wrapper

    def reset(self) -> None:
        """
        Manually reset the circuit breaker to CLOSED state.

        Example:
            >>> breaker.reset()  # Force circuit closed
        """
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._last_failure_time = None

    def get_stats(self) -> dict:
        """
        Get circuit breaker statistics.

        Returns:
            Dictionary with state, failure count, etc.

        Example:
            >>> stats = breaker.get_stats()
            >>> print(f"State: {stats['state']}, Failures: {stats['failure_count']}")
        """
        with self._lock:
            return {
                "name": self.name,
                "state": self._state.value,
                "failure_count": self._failure_count,
                "success_count": self._success_count,
                "failure_threshold": self.failure_threshold,
                "timeout": self.timeout,
                "last_failure_time": self._last_failure_time,
            }

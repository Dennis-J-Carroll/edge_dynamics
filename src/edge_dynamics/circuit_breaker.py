#!/usr/bin/env python3
"""
Circuit breaker implementation for edge_dynamics.

This module implements the circuit breaker pattern to improve system resilience
by preventing cascading failures when the collector is unavailable.
"""

import time
import threading
from enum import Enum
from typing import Callable, Any, Optional
from functools import wraps


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "CLOSED"      # Normal operation
    OPEN = "OPEN"          # Failing, rejecting calls
    HALF_OPEN = "HALF_OPEN"  # Testing recovery


class CircuitBreaker:
    """
    Circuit breaker implementation for fault tolerance.
    
    The circuit breaker monitors calls to a protected function and:
    - Tracks failures and successes
    - Opens the circuit when failure threshold is exceeded
    - Automatically attempts to recover in HALF_OPEN state
    - Rejects calls when circuit is OPEN to prevent cascading failures
    
    Example:
        >>> breaker = CircuitBreaker(failure_threshold=5, timeout=60)
        >>> @breaker
        ... def send_to_collector(data):
        ...     # Network call that might fail
        ...     return collector.send(data)
    """
    
    def __init__(self, 
                 failure_threshold: int = 5,
                 timeout: float = 60.0,
                 success_threshold: int = 3,
                 name: str = "CircuitBreaker"):
        """
        Initialize circuit breaker.
        
        Args:
            failure_threshold: Number of failures before opening circuit
            timeout: Time in seconds to wait before attempting recovery
            success_threshold: Number of successes needed to close circuit
            name: Name for logging and identification
        """
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.success_threshold = success_threshold
        self.name = name
        
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._lock = threading.RLock()
        
        # Statistics
        self._total_calls = 0
        self._total_failures = 0
        self._total_successes = 0
    
    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                if self._last_failure_time and time.time() - self._last_failure_time > self.timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._success_count = 0
            return self._state
    
    @property
    def stats(self) -> dict:
        """Get circuit breaker statistics."""
        with self._lock:
            return {
                "name": self.name,
                "state": self._state.value,
                "failure_count": self._failure_count,
                "failure_threshold": self.failure_threshold,
                "total_calls": self._total_calls,
                "total_failures": self._total_failures,
                "total_successes": self._total_successes,
                "last_failure_time": self._last_failure_time,
            }
    
    def __call__(self, func: Callable) -> Callable:
        """Decorator to wrap functions with circuit breaker protection."""
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            return self._call_with_circuit_breaker(func, *args, **kwargs)
        
        # Add circuit breaker reference to wrapper
        wrapper.circuit_breaker = self
        return wrapper
    
    def _call_with_circuit_breaker(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection."""
        
        with self._lock:
            self._total_calls += 1
            current_state = self.state
            
            if current_state == CircuitState.OPEN:
                raise Exception(f"Circuit breaker '{self.name}' is OPEN. "
                               f"Call rejected to prevent cascading failure.")
        
        try:
            # Call the protected function
            result = func(*args, **kwargs)
            
            # Record success
            self._record_success()
            
            return result
            
        except Exception as e:
            # Record failure
            self._record_failure()
            raise e
    
    def _record_success(self):
        """Record a successful call."""
        with self._lock:
            self._total_successes += 1
            
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.success_threshold:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._success_count = 0
            elif self._state == CircuitState.CLOSED:
                self._failure_count = 0
    
    def _record_failure(self):
        """Record a failed call."""
        with self._lock:
            self._total_failures += 1
            self._failure_count += 1
            self._last_failure_time = time.time()
            
            if self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN
    
    def force_open(self):
        """Manually open the circuit breaker."""
        with self._lock:
            self._state = CircuitState.OPEN
            self._failure_count = self.failure_threshold
            self._last_failure_time = time.time()
    
    def force_close(self):
        """Manually close the circuit breaker."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._last_failure_time = None


class CircuitBreakerManager:
    """
    Manages multiple circuit breakers for different services.
    """
    
    def __init__(self):
        self._circuit_breakers: dict[str, CircuitBreaker] = {}
        self._lock = threading.RLock()
    
    def get_or_create(self, name: str, **kwargs) -> CircuitBreaker:
        """Get existing circuit breaker or create a new one."""
        with self._lock:
            if name not in self._circuit_breakers:
                self._circuit_breakers[name] = CircuitBreaker(name=name, **kwargs)
            return self._circuit_breakers[name]
    
    def get_stats(self) -> dict:
        """Get statistics for all circuit breakers."""
        with self._lock:
            return {
                name: cb.stats
                for name, cb in self._circuit_breakers.items()
            }
    
    def health_check(self) -> dict:
        """Check health of all circuit breakers."""
        stats = self.get_stats()
        
        open_circuits = [
            name for name, stat in stats.items()
            if stat["state"] == CircuitState.OPEN.value
        ]
        
        return {
            "healthy": len(open_circuits) == 0,
            "total_circuits": len(stats),
            "open_circuits": open_circuits,
            "circuit_details": stats
        }


# Global circuit breaker manager
_circuit_manager: Optional[CircuitBreakerManager] = None


def get_circuit_manager() -> CircuitBreakerManager:
    """Get the global circuit breaker manager."""
    global _circuit_manager
    if _circuit_manager is None:
        _circuit_manager = CircuitBreakerManager()
    return _circuit_manager


# Example usage
if __name__ == "__main__":
    # Create a circuit breaker
    breaker = CircuitBreaker(
        failure_threshold=3,
        timeout=10.0,
        name="collector_connection"
    )
    
    # Use as decorator
    @breaker
    def unreliable_network_call(data):
        """Simulate an unreliable network call."""
        import random
        if random.random() < 0.7:  # 70% failure rate
            raise ConnectionError("Network failure")
        return f"Success: {data}"
    
    # Test the circuit breaker
    for i in range(10):
        try:
            result = unreliable_network_call(f"data_{i}")
            print(f"Call {i}: {result}")
        except Exception as e:
            print(f"Call {i}: Failed - {e}")
        
        time.sleep(0.5)
        
        # Print circuit state
        print(f"  Circuit state: {breaker.state.value}")
        print(f"  Stats: {breaker.stats}")
        print()

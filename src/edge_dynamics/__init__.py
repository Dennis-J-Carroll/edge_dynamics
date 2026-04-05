# SPDX-License-Identifier: Apache-2.0
"""
edge_dynamics — Edge-side compression using per-topic dictionaries
and batching for IoT telemetry.
"""

__version__ = "0.1.0"

from edge_dynamics.config import Settings, get_settings, reload_settings
from edge_dynamics.structured_logging import StructuredLogger, get_logger
from edge_dynamics.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerManager,
    CircuitState,
    get_circuit_manager,
)
from edge_dynamics.disk_buffer import DiskBuffer
from edge_dynamics.security import (
    sign_frame,
    verify_frame,
    create_client_context,
    create_server_context,
)

__all__ = [
    # version
    "__version__",
    # config
    "Settings",
    "get_settings",
    "reload_settings",
    # logging
    "StructuredLogger",
    "get_logger",
    # circuit breaker
    "CircuitBreaker",
    "CircuitBreakerManager",
    "CircuitState",
    "get_circuit_manager",
    # disk buffer
    "DiskBuffer",
    # security
    "sign_frame",
    "verify_frame",
    "create_client_context",
    "create_server_context",
]

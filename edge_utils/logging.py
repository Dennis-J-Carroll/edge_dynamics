# SPDX-License-Identifier: Apache-2.0
"""
Structured logging module for edge_dynamics.

Provides JSON-formatted logging with context enrichment for better
observability in production environments.

Example:
    >>> from edge_utils.logging import get_logger
    >>> logger = get_logger("edge_agent")
    >>> logger.info("batch_flushed", topic="sensors.temp", count=100, ratio=0.25)
"""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional


class StructuredFormatter(logging.Formatter):
    """Custom formatter that outputs JSON-structured logs."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_entry: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add extra fields from the record
        if hasattr(record, "extra_fields"):
            log_entry.update(record.extra_fields)

        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, separators=(",", ":"))


class StructuredLogger:
    """
    Structured logger that outputs JSON-formatted logs.

    Attributes:
        logger: The underlying Python logger instance
        name: Logger name/identifier
    """

    def __init__(self, name: str, level: int = logging.INFO):
        """
        Initialize structured logger.

        Args:
            name: Logger name (e.g., "edge_agent", "collector")
            level: Logging level (default: INFO)
        """
        self.logger = logging.getLogger(name)
        self.name = name

        # Avoid adding handlers multiple times
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(StructuredFormatter())
            self.logger.addHandler(handler)
            self.logger.setLevel(level)
            self.logger.propagate = False

    def _log(self, level: str, message: str, **kwargs: Any) -> None:
        """
        Internal method to log with extra fields.

        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            message: Log message
            **kwargs: Additional fields to include in the log entry
        """
        log_method = getattr(self.logger, level.lower())

        # Create a LogRecord with extra fields
        extra = {"extra_fields": kwargs} if kwargs else {}
        log_method(message, extra=extra)

    def debug(self, message: str, **kwargs: Any) -> None:
        """Log debug message with optional fields."""
        self._log("DEBUG", message, **kwargs)

    def info(self, message: str, **kwargs: Any) -> None:
        """Log info message with optional fields."""
        self._log("INFO", message, **kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        """Log warning message with optional fields."""
        self._log("WARNING", message, **kwargs)

    def error(self, message: str, **kwargs: Any) -> None:
        """Log error message with optional fields."""
        self._log("ERROR", message, **kwargs)

    def critical(self, message: str, **kwargs: Any) -> None:
        """Log critical message with optional fields."""
        self._log("CRITICAL", message, **kwargs)

    def exception(self, message: str, **kwargs: Any) -> None:
        """Log exception with traceback."""
        self.logger.exception(message, extra={"extra_fields": kwargs} if kwargs else {})


# Global logger cache
_loggers: Dict[str, StructuredLogger] = {}


def get_logger(name: str, level: int = logging.INFO) -> StructuredLogger:
    """
    Get or create a structured logger instance.

    Args:
        name: Logger name
        level: Logging level (default: INFO)

    Returns:
        StructuredLogger instance

    Example:
        >>> logger = get_logger("edge_agent")
        >>> logger.info("agent_started", version="1.0.0", pid=12345)
    """
    if name not in _loggers:
        _loggers[name] = StructuredLogger(name, level)
    return _loggers[name]

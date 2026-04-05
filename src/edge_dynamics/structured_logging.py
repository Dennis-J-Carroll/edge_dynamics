#!/usr/bin/env python3
"""
Structured logging implementation for edge_dynamics.
This module provides structured logging capabilities to replace print statements.
"""

import logging
import json
import time
import os
from datetime import datetime
from typing import Any, Dict, Optional


class StructuredLogger:
    """
    A structured logger that outputs JSON-formatted log entries.
    
    This logger provides a way to log structured data that can be easily
    parsed by log aggregation systems like ELK, Splunk, or CloudWatch.
    """
    
    def __init__(self, name: str, level: str = "INFO"):
        self.name = name
        self.logger = logging.getLogger(name)
        
        # Remove any existing handlers
        self.logger.handlers = []
        
        # Create handler with JSON formatter
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(message)s'))
        
        self.logger.addHandler(handler)
        self.logger.setLevel(getattr(logging, level.upper()))
        self.logger.propagate = False
    
    def _log(self, level: str, event: str, **kwargs):
        """Create a structured log entry."""
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": level,
            "event": event,
            "service": self.name,
            "pid": os.getpid() if hasattr(os, 'getpid') else None,
            **kwargs
        }
        
        # Remove None values and ensure all values are serializable
        def serialize(obj):
            if isinstance(obj, (str, int, float, bool, type(None))):
                return obj
            if isinstance(obj, dict):
                return {str(k): serialize(v) for k, v in obj.items()}
            if isinstance(obj, (list, tuple, set)):
                return [serialize(i) for i in obj]
            return str(obj)

        log_entry = {k: serialize(v) for k, v in log_entry.items() if v is not None}
        
        self.logger.log(getattr(logging, level.upper()), json.dumps(log_entry))
    
    def debug(self, event: str, **kwargs):
        """Log a debug event."""
        self._log("DEBUG", event, **kwargs)
    
    def info(self, event: str, **kwargs):
        """Log an info event."""
        self._log("INFO", event, **kwargs)
    
    def warning(self, event: str, **kwargs):
        """Log a warning event."""
        self._log("WARNING", event, **kwargs)
    
    def error(self, event: str, error: Optional[Exception] = None, **kwargs):
        """Log an error event."""
        if error:
            kwargs["error"] = str(error)
            kwargs["error_type"] = type(error).__name__
        self._log("ERROR", event, **kwargs)
    
    def critical(self, event: str, error: Optional[Exception] = None, **kwargs):
        """Log a critical event."""
        if error:
            kwargs["error"] = str(error)
            kwargs["error_type"] = type(error).__name__
        self._log("CRITICAL", event, **kwargs)


# Global logger instance
logger: Optional[StructuredLogger] = None


def get_logger(name: str = "edge_dynamics") -> StructuredLogger:
    """Get or create a global logger instance."""
    global logger
    if logger is None:
        logger = StructuredLogger(name)
    return logger


# Example usage
if __name__ == "__main__":
    log = get_logger("test")
    
    # Log a simple event
    log.info("service_started", version="1.0.0", port=7000)
    
    # Log with metrics
    log.info("batch_processed", 
             topic="sensors.temperature",
             message_count=100,
             compression_ratio=0.75,
             duration_ms=25.5)
    
    # Log an error
    try:
        raise ValueError("Invalid compression level")
    except Exception as e:
        log.error("compression_failed", 
                  error=e,
                  topic="sensors.temperature",
                  attempted_level=25)

# SPDX-License-Identifier: Apache-2.0
"""Tests for edge_utils.logging module."""

import json
import logging
from io import StringIO

import pytest
from edge_utils.logging import StructuredLogger, get_logger


class TestStructuredLogger:
    """Tests for StructuredLogger class."""

    def test_logger_outputs_json(self, caplog):
        """Test that logger outputs valid JSON."""
        logger = StructuredLogger("test_logger")

        with caplog.at_level(logging.INFO):
            logger.info("test_message", key="value", count=42)

        # Check that log output is valid JSON
        assert len(caplog.records) == 1
        log_output = caplog.records[0].getMessage()

        # Should be able to parse as JSON
        log_data = json.loads(log_output)
        assert log_data["message"] == "test_message"
        assert log_data["level"] == "INFO"
        assert log_data["key"] == "value"
        assert log_data["count"] == 42

    def test_logger_includes_timestamp(self, caplog):
        """Test that logger includes ISO format timestamp."""
        logger = StructuredLogger("test_logger")

        with caplog.at_level(logging.INFO):
            logger.info("test_message")

        log_data = json.loads(caplog.records[0].getMessage())
        assert "timestamp" in log_data
        # Verify it's an ISO timestamp
        assert "T" in log_data["timestamp"]

    def test_logger_debug_level(self, caplog):
        """Test debug level logging."""
        logger = StructuredLogger("test_logger", level=logging.DEBUG)

        with caplog.at_level(logging.DEBUG):
            logger.debug("debug_message", detail="debug_info")

        log_data = json.loads(caplog.records[0].getMessage())
        assert log_data["level"] == "DEBUG"
        assert log_data["detail"] == "debug_info"

    def test_get_logger_returns_cached_instance(self):
        """Test that get_logger returns cached instances."""
        logger1 = get_logger("test")
        logger2 = get_logger("test")
        assert logger1 is logger2


class TestLoggingLevels:
    """Tests for different logging levels."""

    def test_info_logging(self, caplog):
        """Test INFO level logging."""
        logger = get_logger("info_test")
        with caplog.at_level(logging.INFO):
            logger.info("info_message", status="ok")

        log_data = json.loads(caplog.records[0].getMessage())
        assert log_data["level"] == "INFO"

    def test_warning_logging(self, caplog):
        """Test WARNING level logging."""
        logger = get_logger("warn_test")
        with caplog.at_level(logging.WARNING):
            logger.warning("warning_message", code=500)

        log_data = json.loads(caplog.records[0].getMessage())
        assert log_data["level"] == "WARNING"

    def test_error_logging(self, caplog):
        """Test ERROR level logging."""
        logger = get_logger("error_test")
        with caplog.at_level(logging.ERROR):
            logger.error("error_message", error_code=404)

        log_data = json.loads(caplog.records[0].getMessage())
        assert log_data["level"] == "ERROR"

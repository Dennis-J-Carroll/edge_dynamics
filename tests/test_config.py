# SPDX-License-Identifier: Apache-2.0
"""Test configuration and logging management."""

import os
import pytest
import json
from edge_dynamics.config import Settings
from edge_dynamics.structured_logging import StructuredLogger


class TestConfiguration:
    """Test configuration management."""
    
    def test_default_settings(self):
        """Test that default settings are loaded correctly."""
        config = Settings()
        
        assert config.collector_host == "127.0.0.1"
        assert config.collector_port == 7000
        assert config.batch_max == 100
        assert config.batch_ms == 250
        assert config.compression_level == 7
        assert config.dict_dir == "./dicts"
        assert config.out_dir == "./out"
    
    def test_environment_variable_override(self):
        """Test that environment variables override defaults."""
        os.environ["EDGE_COLLECTOR_HOST"] = "192.168.1.100"
        os.environ["EDGE_COLLECTOR_PORT"] = "8000"
        os.environ["EDGE_BATCH_MAX"] = "200"
        os.environ["EDGE_COMPRESSION_LEVEL"] = "15"
        
        try:
            config = Settings()
            
            assert config.collector_host == "192.168.1.100"
            assert config.collector_port == 8000
            assert config.batch_max == 200
            assert config.compression_level == 15
            
        finally:
            del os.environ["EDGE_COLLECTOR_HOST"]
            del os.environ["EDGE_COLLECTOR_PORT"]
            del os.environ["EDGE_BATCH_MAX"]
            del os.environ["EDGE_COMPRESSION_LEVEL"]
    
    def test_validation_errors(self):
        """Test that invalid values raise validation errors."""
        with pytest.raises(ValueError):
            Settings(compression_level=25)  # Too high
        
        with pytest.raises(ValueError):
            Settings(collector_port=70000)  # Too high
        
        with pytest.raises(ValueError):
            Settings(batch_max=-1)  # Negative
        
        with pytest.raises(ValueError):
            Settings(log_level="INVALID")  # Invalid log level


class TestStructuredLogging:
    """Test structured logging functionality."""
    
    def test_logger_creates_json_output(self):
        """Test that logger outputs valid JSON."""
        logger = StructuredLogger("test")
        
        import io
        import logging
        
        log_capture = io.StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setFormatter(logging.Formatter('%(message)s'))
        
        logger.logger.handlers = [handler]
        logger.logger.setLevel(logging.INFO)
        
        logger.info("test_event", key="value", number=42)
        
        log_output = log_capture.getvalue().strip()
        log_data = json.loads(log_output)
        
        assert "timestamp" in log_data
        assert log_data["level"] == "INFO"
        assert log_data["event"] == "test_event"
        assert log_data["service"] == "test"
        assert log_data["key"] == "value"
        assert log_data["number"] == 42
    
    def test_logger_error_includes_exception_info(self):
        """Test that error logging includes exception information."""
        logger = StructuredLogger("test")
        
        import io
        import logging
        
        log_capture = io.StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setFormatter(logging.Formatter('%(message)s'))
        
        logger.logger.handlers = [handler]
        logger.logger.setLevel(logging.ERROR)
        
        try:
            raise ValueError("Test error")
        except Exception as e:
            logger.error("operation_failed", error=e, context="testing")
        
        log_output = log_capture.getvalue().strip()
        log_data = json.loads(log_output)
        
        assert log_data["level"] == "ERROR"
        assert log_data["error"] == "Test error"
        assert log_data["error_type"] == "ValueError"
        assert log_data["context"] == "testing"

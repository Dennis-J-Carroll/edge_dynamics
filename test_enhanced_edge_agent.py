#!/usr/bin/env python3
"""
Enhanced test suite for edge_agent with comprehensive coverage.

This test suite demonstrates the testing improvements recommended
in the evaluation report.
"""

import pytest
import time
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock, call
from collections import deque
import json
import zstandard as zstd

# Import the modules we want to test
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from edge_agent import EdgeAgent, normalize_message
from structured_logging import StructuredLogger, get_logger
from config import Settings, get_settings
from circuit_breaker import CircuitBreaker, get_circuit_manager, CircuitState
from disk_buffer import DiskBuffer


class TestDiskBuffer:
    """Test persistent disk buffer functionality."""
    
    def test_disk_buffer_push_pop(self):
        """Test basic push and pop operations."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name
        
        try:
            buffer = DiskBuffer(db_path, max_mb=1)
            
            # Push some data
            buffer.push("topic1", b"payload1")
            buffer.push("topic2", b"payload2")
            
            assert buffer.get_count() == 2
            
            # Pop data
            batches = buffer.pop_batch(limit=1)
            assert len(batches) == 1
            assert batches[0][1] == "topic1"
            assert batches[0][2] == b"payload1"
            
            assert buffer.get_count() == 1
            
            batches = buffer.pop_batch(limit=1)
            assert len(batches) == 1
            assert batches[0][1] == "topic2"
            
            assert buffer.get_count() == 0
        finally:
            if os.path.exists(db_path):
                os.remove(db_path)

    def test_disk_buffer_fifo_eviction(self):
        """Test that oldest records are deleted when limit exceeded."""
        # Use a slightly larger limit but still small enough to trigger
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name
        
        try:
            # 0.1 MB is about 100KB
            buffer = DiskBuffer(db_path, max_mb=0.1)
            
            # Push enough data to exceed 100KB
            # 100 * 5KB = 500KB
            large_payload = b"x" * 5120
            for i in range(100):
                buffer.push(f"topic{i}", large_payload)
            
            # Count should be less than 100 if eviction worked
            count = buffer.get_count()
            assert count < 100
            assert count > 0 # Should NOT be zero
            
            # Verify we have the NEWEST items (Topic 99 should be there)
            batches = buffer.pop_batch(limit=100)
            topics = [b[1] for b in batches]
            assert f"topic99" in topics
        finally:
            if os.path.exists(db_path):
                os.remove(db_path)


class TestStoreAndForward:
    """Test integration between EdgeAgent and DiskBuffer."""
    
    @patch('edge_agent.socket.create_connection')
    def test_buffering_on_failure(self, mock_socket):
        """Test that batches are buffered when sending fails."""
        mock_socket.side_effect = Exception("Connection refused")
        
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name
        
        try:
            with patch('edge_agent.load_dictionaries'), patch('edge_agent.build_compressors'):
                # Use real settings to avoid MagicMock serialization errors
                from config import Settings
                test_settings = Settings(
                    disk_buffer_enabled=True,
                    disk_buffer_path=db_path,
                    disk_buffer_max_mb=50,
                    batch_max=1,
                    batch_ms=1000
                )
                
                with patch('edge_agent.settings', test_settings):
                    agent = EdgeAgent(enable_flush=False, enable_metrics=False, enable_health=False)
                    
                    # This should trigger a flush which fails and buffers
                    agent.enqueue("test.topic", {"data": "test"})
                    
                    # Verify it's in the buffer
                    assert agent.disk_buffer.get_count() == 1
        finally:
            if os.path.exists(db_path):
                os.remove(db_path)


class TestNormalizeMessage:
    """Test message normalization functionality."""
    
    def test_normalize_removes_trace_id(self):
        """Test that X-Amzn-Trace-Id is removed from headers."""
        msg = {
            "headers": {
                "Accept": "*/*",
                "User-Agent": "test-agent",
                "X-Amzn-Trace-Id": "Root=1-12345-67890",
            },
            "data": "test",
            "timestamp": 1640995200
        }
        
        result = normalize_message(msg)
        result_dict = json.loads(result.decode())
        
        assert "X-Amzn-Trace-Id" not in result_dict["headers"]
        assert result_dict["headers"]["Accept"] == "*/*"
        assert result_dict["headers"]["User-Agent"] == "test-agent"
        assert result_dict["data"] == "test"
        assert result_dict["timestamp"] == 1640995200
    
    def test_normalize_handles_missing_headers(self):
        """Test that messages without headers are handled correctly."""
        msg = {"data": "test", "value": 42}
        
        result = normalize_message(msg)
        result_dict = json.loads(result.decode())
        
        assert result_dict == {"data": "test", "value": 42}
    
    def test_normalize_produces_compact_json(self):
        """Test that normalized JSON has no unnecessary spaces."""
        msg = {
            "key": "value",
            "nested": {"a": 1, "b": 2},
            "list": [1, 2, 3]
        }
        
        result = normalize_message(msg)
        result_str = result.decode()
        
        # Should not have spaces after separators
        assert ": " not in result_str
        assert ", " not in result_str
        assert result_str.startswith(b'{'.decode())
        assert result_str.endswith(b'}'.decode())
    
    def test_normalize_preserves_data_types(self):
        """Test that different data types are preserved."""
        msg = {
            "string": "test",
            "integer": 42,
            "float": 3.14,
            "boolean": True,
            "null": None,
            "list": [1, 2, 3],
            "object": {"nested": "value"}
        }
        
        result = normalize_message(msg)
        result_dict = json.loads(result.decode())
        
        assert result_dict["string"] == "test"
        assert result_dict["integer"] == 42
        assert result_dict["float"] == 3.14
        assert result_dict["boolean"] is True
        assert result_dict["null"] is None
        assert result_dict["list"] == [1, 2, 3]
        assert result_dict["object"]["nested"] == "value"


class TestEdgeAgentInitialization:
    """Test EdgeAgent initialization and setup."""
    
    @patch('edge_agent.load_dictionaries')
    @patch('edge_agent.build_compressors')
    def test_initialization_loads_dictionaries(self, mock_build, mock_load):
        """Test that dictionaries are loaded during initialization."""
        # Mock dictionary loading
        mock_dict_index = {
            "files.txt": {"dict_id": "txt123", "path": "dicts/files.txt.dict"},
            "files.csv": {"dict_id": "csv456", "path": "dicts/files.csv.dict"}
        }
        mock_compressors = {
            "files.txt": {"dict_id": "txt123", "compressor": Mock()},
            "files.csv": {"dict_id": "csv456", "compressor": Mock()}
        }
        
        mock_load.return_value = mock_dict_index
        mock_build.return_value = mock_compressors
        
        # Create agent
        agent = EdgeAgent()
        
        # Verify dictionaries were loaded
        assert agent.dict_index == mock_dict_index
        assert agent.compressors == mock_compressors
        mock_load.assert_called_once()
        mock_build.assert_called_once_with(mock_dict_index)
    
    def test_initialization_creates_buffers(self):
        """Test that message buffers are properly initialized."""
        with patch('edge_agent.load_dictionaries'), patch('edge_agent.build_compressors'):
            agent = EdgeAgent(enable_flush=False, enable_metrics=False, enable_health=False)

            # Should have default dict for buffers
            assert isinstance(agent.buffers, dict)

            # Should have lock and flusher thread
            assert agent.lock is not None
            assert agent.flusher_thread is not None
            assert agent.flusher_thread.daemon is True


class TestEdgeAgentBatching:
    """Test message batching and flushing behavior."""

    def test_enqueue_adds_message_to_buffer(self):
        """Test that messages are added to the correct topic buffer."""
        with patch('edge_agent.load_dictionaries'), patch('edge_agent.build_compressors'):
            agent = EdgeAgent(enable_flush=False, enable_metrics=False, enable_health=False)

            # Add message
            msg = {"data": "test", "timestamp": time.time()}
            agent.enqueue("test.topic", msg)

            # Verify message is in buffer
            assert "test.topic" in agent.buffers
            buffer = agent.buffers["test.topic"]
            assert len(buffer["q"]) == 1
            assert buffer["t0"] is not None

    @patch('edge_agent.EdgeAgent._flush_batch')
    def test_flush_triggered_by_batch_size(self, mock_flush):
        """Test that batches flush when reaching size threshold."""
        from edge_agent import settings
        with patch('edge_agent.load_dictionaries'), patch('edge_agent.build_compressors'):
            agent = EdgeAgent(enable_flush=False, enable_metrics=False, enable_health=False)

            # Add messages up to batch max - 1
            batch_max = settings.batch_max
            for i in range(batch_max - 1):
                agent.enqueue("test.topic", {"id": i})

            # Should not have flushed yet
            assert mock_flush.call_count == 0

            # Add one more message to trigger flush
            agent.enqueue("test.topic", {"id": batch_max})

            # Should have flushed once
            assert mock_flush.call_count == 1

    @patch('edge_agent.EdgeAgent._flush_batch')
    def test_flush_triggered_by_timeout(self, mock_flush):
        """Test that batches flush after timeout even if not full."""
        from edge_agent import settings
        with patch('edge_agent.load_dictionaries'), patch('edge_agent.build_compressors'):
            # Use smaller timeout for testing
            with patch.object(settings, 'batch_ms', 50):  # 50ms
                agent = EdgeAgent(enable_metrics=False, enable_health=False)
                agent.start()

                # Add a few messages
                agent.enqueue("test.topic", {"id": 1})
                agent.enqueue("test.topic", {"id": 2})

                # Should not flush immediately
                assert mock_flush.call_count == 0

                # Poll for flush with timeout
                max_wait = 2.0
                start_time = time.time()
                while mock_flush.call_count == 0 and (time.time() - start_time) < max_wait:
                    time.sleep(0.05)

                # Should have flushed due to timeout
                assert mock_flush.call_count >= 1

    def test_multiple_topics_isolated(self):
        """Test that different topics have isolated buffers."""
        with patch('edge_agent.load_dictionaries'), patch('edge_agent.build_compressors'):
            agent = EdgeAgent(enable_flush=False, enable_metrics=False, enable_health=False)

            # Add messages to different topics
            for i in range(10):
                agent.enqueue("topic1", {"id": i, "topic": "topic1"})
                agent.enqueue("topic2", {"id": i, "topic": "topic2"})

            # Each topic should have its own buffer
            assert "topic1" in agent.buffers
            assert "topic2" in agent.buffers
            assert len(agent.buffers["topic1"]["q"]) == 10
            assert len(agent.buffers["topic2"]["q"]) == 10


class TestEdgeAgentCompression:
    """Test compression functionality."""

    @patch('edge_agent.socket.create_connection')
    @patch('edge_agent.time.time')
    def test_compression_with_dictionary(self, mock_time, mock_socket):
        """Test compression using per-topic dictionary."""
        with patch('edge_agent.load_dictionaries'), patch('edge_agent.build_compressors'):
            # Setup
            mock_time.return_value = 1640995200.0

            # Create mock dictionary and compressor
            mock_dict = Mock()
            mock_compressor = Mock()
            mock_compressor.compress.return_value = b"compressed_data"

            agent = EdgeAgent(enable_flush=False, enable_metrics=False, enable_health=False)
            agent.compressors = {
                "files.txt": {
                    "dict_id": "txt123",
                    "compressor": mock_compressor
                }
            }

            # Create test messages
            messages = [
                normalize_message({"data": f"test_{i}"})
                for i in range(5)
            ]

            # Call flush batch
            agent._flush_batch("files.txt", messages)

            # Verify compression was called
            raw_data = b'\n'.join(messages) + b'\n'
            mock_compressor.compress.assert_called_once_with(raw_data)

            # Verify socket operations
            mock_socket.assert_called_once()
            mock_socket.return_value.sendall.assert_called_once()

    @patch('edge_agent.socket.create_connection')
    def test_compression_without_dictionary(self, mock_socket):
        """Test fallback when no dictionary is available for topic."""
        with patch('edge_agent.load_dictionaries'), patch('edge_agent.build_compressors'):
            agent = EdgeAgent(enable_flush=False, enable_metrics=False, enable_health=False)
            agent.compressors = {}  # No compressors

            messages = [
                normalize_message({"data": f"test_{i}"})
                for i in range(3)
            ]

            agent._flush_batch("unknown.topic", messages)

            # Should send uncompressed data
            mock_socket.assert_called_once()
            args, kwargs = mock_socket.return_value.sendall.call_args
            frame = args[0]

            # Parse frame to verify no compression
            header_len = int.from_bytes(frame[:4], byteorder='big')
            header_data = frame[4:4+header_len]
            header = json.loads(header_data.decode())

            assert header["dict_id"] == ""  # No dictionary
            assert header["codec"] == "zstd"  # But still using zstd format

class TestStructuredLogging:
    """Test structured logging functionality."""
    
    def test_logger_creates_json_output(self):
        """Test that logger outputs valid JSON."""
        logger = StructuredLogger("test")
        
        # Capture log output
        import io
        import logging
        
        log_capture = io.StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setFormatter(logging.Formatter('%(message)s'))
        
        logger.logger.handlers = [handler]
        logger.logger.setLevel(logging.INFO)
        
        # Log a message
        logger.info("test_event", key="value", number=42)
        
        # Parse the output
        log_output = log_capture.getvalue().strip()
        log_data = json.loads(log_output)
        
        # Verify structure
        assert "timestamp" in log_data
        assert log_data["level"] == "INFO"
        assert log_data["event"] == "test_event"
        assert log_data["service"] == "test"
        assert log_data["key"] == "value"
        assert log_data["number"] == 42
    
    def test_logger_error_includes_exception_info(self):
        """Test that error logging includes exception information."""
        logger = StructuredLogger("test")
        
        # Capture log output
        import io
        import logging
        
        log_capture = io.StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setFormatter(logging.Formatter('%(message)s'))
        
        logger.logger.handlers = [handler]
        logger.logger.setLevel(logging.ERROR)
        
        # Log an error with exception
        try:
            raise ValueError("Test error")
        except Exception as e:
            logger.error("operation_failed", error=e, context="testing")
        
        # Parse the output
        log_output = log_capture.getvalue().strip()
        log_data = json.loads(log_output)
        
        # Verify error information
        assert log_data["level"] == "ERROR"
        assert log_data["error"] == "Test error"
        assert log_data["error_type"] == "ValueError"
        assert log_data["context"] == "testing"


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
        # Set environment variables
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
            # Clean up environment variables
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
        
        # Create a successful function
        @breaker
        def successful_function():
            return "success"
        
        # Multiple calls should remain CLOSED
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
            if call_count <= 5:  # Fail first 5 calls
                raise ConnectionError("Simulated failure")
            return "success"
        
        # First 3 failures should open the circuit
        for i in range(3):
            with pytest.raises(ConnectionError):
                failing_function()
        
        # Circuit should now be OPEN
        assert breaker.state == CircuitState.OPEN
        
        # Next call should be rejected immediately
        with pytest.raises(Exception, match="Circuit breaker.*is OPEN"):
            failing_function()
    
    def test_circuit_resets_after_timeout(self):
        """Test that circuit attempts recovery after timeout."""
        breaker = CircuitBreaker(
            name="test",
            failure_threshold=2,
            timeout=0.1  # Very short timeout for testing
        )
        
        # Open the circuit
        @breaker
        def failing_function():
            raise ConnectionError("Failure")
        
        for i in range(2):
            with pytest.raises(ConnectionError):
                failing_function()
        
        assert breaker.state == CircuitState.OPEN
        
        # Wait for timeout
        time.sleep(0.15)
        
        # Circuit should now be HALF_OPEN
        assert breaker.state == CircuitState.HALF_OPEN
    
    def test_manual_control(self):
        """Test manual control of circuit breaker."""
        breaker = CircuitBreaker(name="test")
        
        # Initially closed
        assert breaker.state == CircuitState.CLOSED
        
        # Force open
        breaker.force_open()
        assert breaker.state == CircuitState.OPEN
        
        # Force close
        breaker.force_close()
        assert breaker.state == CircuitState.CLOSED


# Integration test
class TestIntegration:
    """Integration tests combining multiple components."""
    
    @patch('edge_agent.socket.create_connection')
    def test_full_pipeline_with_circuit_breaker(self, mock_socket):
        """Test full pipeline with circuit breaker protection."""
        from edge_agent import breaker as global_breaker
        with patch('edge_agent.load_dictionaries'), patch('edge_agent.build_compressors'):
            # Create agent
            agent = EdgeAgent()
            # Disable health thread for testing to avoid port conflicts
            with patch.object(agent, '_health_server'):
                agent.start()
        
                # Process messages
                for i in range(10):
                    msg = {
                        "topic": "sensors.temperature",
                        "value": 20.5 + (i * 0.1),
                        "timestamp": time.time(),
                        "device_id": f"sensor_{i % 3}"
                    }
                    agent.enqueue("sensors.temperature", msg)
        
                # Give time for processing
                time.sleep(0.5)
        
                # Verify socket was called
                assert mock_socket.called
        
                # Verify stats
                stats = global_breaker.stats
                assert stats["total_calls"] > 0
            assert stats["state"] == "CLOSED"


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])

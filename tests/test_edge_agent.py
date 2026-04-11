# SPDX-License-Identifier: Apache-2.0
"""Test suite for edge_agent with comprehensive coverage."""

import pytest
import time
import json
from unittest.mock import patch, Mock
import os
import tempfile

from edge_dynamics.edge_agent import EdgeAgent, normalize_message

class TestStoreAndForward:
    """Test integration between EdgeAgent and DiskBuffer."""
    
    def test_buffering_on_failure(self, isolated_agent, mock_socket):
        """Test that batches are buffered when sending fails."""
        mock_socket.sendall.side_effect = Exception("Connection refused")
        
        agent = isolated_agent
        
        # This should trigger a flush which fails and buffers
        agent.enqueue("test.topic", {"data": "test"})
        agent._flush_batch("test.topic", agent.buffers["test.topic"]["q"])
        
        # Verify it's in the buffer
        # In isolated_agent, settings are set up so disk_buffer should be instantiated
        assert agent.disk_buffer.get_count() == 1


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
        
        assert ": " not in result_str
        assert ", " not in result_str
        assert result_str.startswith('{')
        assert result_str.endswith('}')
    
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
    
    def test_initialization_creates_dlm(self, isolated_agent):
        """Test that DictionaryLifecycleManager is created during initialization."""
        from edge_dynamics.edge_utils.dict_lifecycle import DictionaryLifecycleManager
        assert hasattr(isolated_agent, "dlm")
        assert isinstance(isolated_agent.dlm, DictionaryLifecycleManager)
        # Backwards-compat stubs are empty dicts
        assert isolated_agent.dict_index == {}
        assert isolated_agent.compressors == {}
    
    def test_initialization_creates_buffers(self, isolated_agent):
        """Test that message buffers are properly initialized."""
        agent = isolated_agent
        assert isinstance(agent.buffers, dict)
        assert agent.lock is not None
        assert agent.flusher_thread is not None
        assert agent.flusher_thread.daemon is True


class TestEdgeAgentBatching:
    """Test message batching and flushing behavior."""

    def test_enqueue_adds_message_to_buffer(self, isolated_agent):
        """Test that messages are added to the correct topic buffer."""
        agent = isolated_agent

        msg = {"data": "test", "timestamp": time.time()}
        agent.enqueue("test.topic", msg)

        assert "test.topic" in agent.buffers
        buffer = agent.buffers["test.topic"]
        assert len(buffer["q"]) == 1
        assert buffer["t0"] is not None

    @patch('edge_dynamics.edge_agent.EdgeAgent._flush_batch')
    def test_flush_triggered_by_batch_size(self, mock_flush, isolated_agent):
        """Test that batches flush when reaching size threshold."""
        agent = isolated_agent
        from edge_dynamics.edge_agent import settings
        batch_max = settings.batch_max
        
        for i in range(batch_max - 1):
            agent.enqueue("test.topic", {"id": i})

        assert mock_flush.call_count == 0

        agent.enqueue("test.topic", {"id": batch_max})

        assert mock_flush.call_count == 1

    @patch('edge_dynamics.edge_agent.EdgeAgent._flush_batch')
    def test_flush_triggered_by_timeout(self, mock_flush):
        """Test that batches flush after timeout even if not full."""
        from edge_dynamics.edge_agent import settings
        with patch.object(settings, 'batch_ms', 50):
                agent = EdgeAgent(enable_metrics=False, enable_health=False)
                agent.start()

                agent.enqueue("test.topic", {"id": 1})
                agent.enqueue("test.topic", {"id": 2})

                assert mock_flush.call_count == 0

                max_wait = 2.0
                start_time = time.time()
                while mock_flush.call_count == 0 and (time.time() - start_time) < max_wait:
                    time.sleep(0.05)

                assert mock_flush.call_count >= 1
                
                # Cleanup
                agent.enable_flush = False


    def test_multiple_topics_isolated(self, isolated_agent):
        """Test that different topics have isolated buffers."""
        agent = isolated_agent

        for i in range(10):
            agent.enqueue("topic1", {"id": i, "topic": "topic1"})
            agent.enqueue("topic2", {"id": i, "topic": "topic2"})

        assert "topic1" in agent.buffers
        assert "topic2" in agent.buffers
        assert len(agent.buffers["topic1"]["q"]) == 10
        assert len(agent.buffers["topic2"]["q"]) == 10


class TestEdgeAgentCompression:
    """Test compression functionality."""

    @patch('edge_dynamics.edge_agent.time.time')
    def test_compression_with_dictionary(self, mock_time, mock_socket, isolated_agent):
        """Test compression using per-topic dictionary via DLM."""
        mock_time.return_value = 1640995200.0

        mock_compressor = Mock()
        mock_compressor.compress.return_value = b"compressed_data"

        agent = isolated_agent
        # v2: inject compressor via DLM instead of agent.compressors
        agent.dlm.get_compressor = Mock(return_value={
            "dict_id": "txt123",
            "compressor": mock_compressor,
        })

        messages = [
            normalize_message({"data": f"test_{i}"})
            for i in range(5)
        ]

        agent._flush_batch("files.txt", messages)

        raw_data = b'\n'.join(messages) + b'\n'
        mock_compressor.compress.assert_called_once_with(raw_data)
        mock_socket.sendall.assert_called_once()

    def test_compression_without_dictionary(self, mock_socket, isolated_agent):
        """Test fallback when no dictionary is available for topic."""
        agent = isolated_agent

        messages = [
            normalize_message({"data": f"test_{i}"})
            for i in range(3)
        ]

        agent._flush_batch("unknown.topic", messages)

        args, _ = mock_socket.sendall.call_args
        frame = args[0]

        header_len = int.from_bytes(frame[:4], byteorder='big')
        header_data = frame[4:4+header_len]
        header = json.loads(header_data.decode())

        assert header["dict_id"] == ""
        assert header["codec"] == "zstd"

class TestIntegration:
    """Integration tests combining multiple components."""
    
    def test_full_pipeline_with_circuit_breaker(self, mock_socket):
        """Test full pipeline with circuit breaker protection."""
        from edge_dynamics.edge_agent import breaker as global_breaker
        agent = EdgeAgent()
        with patch.object(agent, '_health_server'):
            agent.start()

            for i in range(10):
                msg = {
                    "topic": "sensors.temperature",
                    "value": 20.5 + (i * 0.1),
                    "timestamp": time.time(),
                    "device_id": f"sensor_{i % 3}"
                }
                agent.enqueue("sensors.temperature", msg)

            time.sleep(0.5)

            assert mock_socket.sendall.called

            stats = global_breaker.stats
            assert stats["total_calls"] > 0
        assert stats["state"] == "CLOSED"

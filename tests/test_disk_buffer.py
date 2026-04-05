# SPDX-License-Identifier: Apache-2.0
"""Test persistent disk buffer functionality."""

import os
import tempfile
import pytest
from edge_dynamics.disk_buffer import DiskBuffer


class TestDiskBuffer:
    """Test persistent disk buffer functionality."""
    
    def test_disk_buffer_push_pop(self, disk_buffer):
        """Test basic push and pop operations."""
        buffer = disk_buffer
        
        buffer.push("topic1", b"payload1")
        buffer.push("topic2", b"payload2")
        
        assert buffer.get_count() == 2
        
        batches = buffer.pop_batch(limit=1)
        assert len(batches) == 1
        assert batches[0][1] == "topic1"
        assert batches[0][2] == b"payload1"
        
        assert buffer.get_count() == 1
        
        batches = buffer.pop_batch(limit=1)
        assert len(batches) == 1
        assert batches[0][1] == "topic2"
        
        assert buffer.get_count() == 0

    def test_disk_buffer_fifo_eviction(self):
        """Test that oldest records are deleted when limit exceeded."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name
        
        try:
            buffer = DiskBuffer(db_path, max_mb=0.1)
            
            large_payload = b"x" * 5120
            for i in range(100):
                buffer.push(f"topic{i}", large_payload)
            
            count = buffer.get_count()
            assert count < 100
            assert count > 0
            
            batches = buffer.pop_batch(limit=100)
            topics = [b[1] for b in batches]
            assert "topic99" in topics
        finally:
            if os.path.exists(db_path):
                os.remove(db_path)

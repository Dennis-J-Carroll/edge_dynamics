# SPDX-License-Identifier: Apache-2.0
"""Pytest configuration and shared fixtures."""

import pytest
import os
import tempfile
from pathlib import Path
from unittest.mock import patch, Mock

from edge_dynamics.config import Settings
from edge_dynamics.edge_agent import EdgeAgent
from edge_dynamics.disk_buffer import DiskBuffer


@pytest.fixture
def temp_dict_dir():
    """Create a temporary directory for dictionary files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_messages():
    """Provide sample messages for testing."""
    return [
        {
            "file_type": "txt",
            "path": "/var/log/files.txt/test.txt",
            "size": 1024,
            "checksum": "abc123",
        },
        {
            "file_type": "csv",
            "path": "/var/log/files.csv/test.csv",
            "size": 2048,
            "checksum": "def456",
        },
        {
            "file_type": "json",
            "path": "/var/log/files.json/test.json",
            "size": 4096,
            "checksum": "ghi789",
        },
    ]

@pytest.fixture
def test_settings(tmp_path):
    """Provide a Settings instance with test values."""
    db_path = str(tmp_path / "test_buffer.db")
    return Settings(
        collector_host="127.0.0.1",
        collector_port=7000,
        disk_buffer_enabled=True,
        disk_buffer_path=db_path,
        disk_buffer_max_mb=50,
        batch_max=100,
        batch_ms=100
    )


@pytest.fixture
def mock_socket():
    """Provide a mock socket that avoids real connections."""
    with patch('edge_dynamics.edge_agent.socket.create_connection') as mock:
        yield mock


@pytest.fixture
def isolated_agent(test_settings):
    """Provide an EdgeAgent with mocking and test settings to isolate it."""
    with patch('edge_dynamics.edge_agent.settings', test_settings):
        with patch('edge_dynamics.edge_agent.load_dictionaries') as mock_load, \
             patch('edge_dynamics.edge_agent.build_compressors') as mock_build:
            
            mock_load.return_value = {}
            mock_build.return_value = {}
            
            agent = EdgeAgent(enable_flush=False, enable_metrics=False, enable_health=False)
            yield agent


@pytest.fixture
def disk_buffer(tmp_path):
    """Provide an isolated DiskBuffer instance."""
    db_path = str(tmp_path / "test_buffer_isolated.db")
    buffer = DiskBuffer(db_path, max_mb=1)
    yield buffer

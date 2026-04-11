# SPDX-License-Identifier: Apache-2.0
"""Pytest configuration and shared fixtures."""

import tempfile

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

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
        batch_ms=100,
        dict_dir=str(tmp_path / "dicts"),
    )


@pytest.fixture
def mock_socket():
    """Mock ConnectionPool so no real TCP connections are made.

    Yields the mock socket returned by pool.get_connection().__enter__().
    Tests that previously checked socket.create_connection behaviour should
    now check mock_socket.sendall.* instead.
    """
    mock_sock = MagicMock()
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = MagicMock(return_value=mock_sock)
    mock_ctx.__exit__ = MagicMock(return_value=False)
    mock_pool = MagicMock()
    mock_pool.get_connection.return_value = mock_ctx
    mock_pool.get_stats.return_value = {}
    mock_pool.close_all = MagicMock()
    with patch('edge_dynamics.edge_agent.ConnectionPool', return_value=mock_pool):
        yield mock_sock


@pytest.fixture
def isolated_agent(test_settings, mock_socket):
    """Provide an EdgeAgent with mocking and test settings to isolate it."""
    with patch('edge_dynamics.edge_agent.settings', test_settings):
        agent = EdgeAgent(enable_flush=False, enable_metrics=False, enable_health=False)
        yield agent


@pytest.fixture
def disk_buffer(tmp_path):
    """Provide an isolated DiskBuffer instance."""
    db_path = str(tmp_path / "test_buffer_isolated.db")
    buffer = DiskBuffer(db_path, max_mb=1)
    yield buffer

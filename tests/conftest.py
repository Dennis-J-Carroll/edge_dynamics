# SPDX-License-Identifier: Apache-2.0
"""Pytest configuration and shared fixtures."""

import pytest
import os
import tempfile
from pathlib import Path


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

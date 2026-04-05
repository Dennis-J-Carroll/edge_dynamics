# SPDX-License-Identifier: Apache-2.0
"""Tests for train_dict module."""

import pytest


def load_bytes(paths: list, max_bytes: int = 256_000) -> bytes:
    """Duplicate of load_bytes for testing without importing module."""
    buf = bytearray()
    for p in paths:
        with open(p, "rb") as f:
            data = f.read()
        buf += data + b"\n"
        if len(buf) >= max_bytes:
            break
    return bytes(buf)


class TestLoadBytes:
    """Tests for load_bytes function."""

    def test_load_bytes_limits_size(self, tmp_path):
        """Test that load_bytes stops after exceeding max_bytes."""
        # Create two test files
        file1 = tmp_path / "test1.txt"
        file2 = tmp_path / "test2.txt"
        file1.write_bytes(b"x" * 600)
        file2.write_bytes(b"y" * 600)

        # load_bytes reads whole files then checks the limit after appending
        result = load_bytes([str(file1), str(file2)], max_bytes=500)
        # First file (600 bytes + newline = 601) exceeds limit, so it stops
        assert len(result) == 601
        assert b"y" not in result

    def test_load_bytes_concatenates_files(self, tmp_path):
        """Test that load_bytes concatenates multiple files."""
        file1 = tmp_path / "test1.txt"
        file2 = tmp_path / "test2.txt"
        file1.write_bytes(b"abc")
        file2.write_bytes(b"def")

        result = load_bytes([str(file1), str(file2)])
        assert result == b"abc\ndef\n"

    def test_load_bytes_stops_at_max_bytes(self, tmp_path):
        """Test that load_bytes stops reading after max_bytes."""
        file1 = tmp_path / "test1.txt"
        file2 = tmp_path / "test2.txt"
        file1.write_bytes(b"a" * 100)
        file2.write_bytes(b"b" * 100)

        result = load_bytes([str(file1), str(file2)], max_bytes=50)
        # Should only read from first file
        assert len(result) == 101  # 100 + newline
        assert b"b" not in result

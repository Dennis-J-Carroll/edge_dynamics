# SPDX-License-Identifier: Apache-2.0
"""Tests for edge_utils.validation module."""

import pytest
from edge_utils.validation import InputValidator, ValidationError


class TestTopicValidation:
    """Tests for topic validation."""

    def test_valid_topic(self):
        """Test that valid topics pass validation."""
        valid_topics = [
            "sensors.temperature",
            "files.txt",
            "my-topic",
            "topic_123",
            "a.b.c.d",
        ]
        for topic in valid_topics:
            result = InputValidator.validate_topic(topic)
            assert result == topic

    def test_reject_path_traversal(self):
        """Test that path traversal is rejected."""
        invalid_topics = [
            "../../../etc/passwd",
            "test/../secret",
            "..hidden",
        ]
        for topic in invalid_topics:
            with pytest.raises(ValidationError, match="Path traversal"):
                InputValidator.validate_topic(topic)

    def test_reject_absolute_paths(self):
        """Test that absolute paths are rejected."""
        with pytest.raises(ValidationError, match="path separator"):
            InputValidator.validate_topic("/etc/passwd")

        with pytest.raises(ValidationError, match="path separator"):
            InputValidator.validate_topic("\\windows\\system32")

    def test_reject_empty_topic(self):
        """Test that empty topic is rejected."""
        with pytest.raises(ValidationError, match="cannot be empty"):
            InputValidator.validate_topic("")

    def test_reject_too_long_topic(self):
        """Test that too-long topics are rejected."""
        long_topic = "a" * 256
        with pytest.raises(ValidationError, match="maximum length"):
            InputValidator.validate_topic(long_topic)

    def test_reject_invalid_characters(self):
        """Test that invalid characters are rejected."""
        invalid_topics = [
            "topic with spaces",
            "topic@special",
            "topic$money",
            "topic#hash",
        ]
        for topic in invalid_topics:
            with pytest.raises(ValidationError):
                InputValidator.validate_topic(topic)


class TestMessageSizeValidation:
    """Tests for message size validation."""

    def test_valid_message_size(self):
        """Test that normal-sized messages pass."""
        data = b"x" * 1000
        result = InputValidator.validate_message_size(data)
        assert result == data

    def test_reject_oversized_message(self):
        """Test that oversized messages are rejected."""
        data = b"x" * (11 * 1024 * 1024)  # 11 MB
        with pytest.raises(ValidationError, match="exceeds maximum"):
            InputValidator.validate_message_size(data)


class TestPortValidation:
    """Tests for port number validation."""

    def test_valid_ports(self):
        """Test that valid ports pass."""
        valid_ports = [80, 443, 8080, 7000, 65535]
        for port in valid_ports:
            result = InputValidator.validate_port(port)
            assert result == port

    def test_reject_invalid_ports(self):
        """Test that invalid ports are rejected."""
        invalid_ports = [0, -1, 65536, 100000]
        for port in invalid_ports:
            with pytest.raises(ValidationError):
                InputValidator.validate_port(port)


class TestPathSanitization:
    """Tests for path sanitization."""

    def test_sanitize_safe_path(self, tmp_path):
        """Test sanitizing a safe path."""
        base_dir = str(tmp_path)
        safe_path = InputValidator.sanitize_path("data.jsonl", base_dir)
        assert safe_path.startswith(base_dir)
        assert safe_path.endswith("data.jsonl")

    def test_reject_path_escape(self, tmp_path):
        """Test that path escapes are rejected."""
        base_dir = str(tmp_path)
        with pytest.raises(ValidationError, match="escape"):
            InputValidator.sanitize_path("../../etc/passwd", base_dir)


class TestHeaderValidation:
    """Tests for header validation."""

    def test_valid_header(self):
        """Test that valid header passes."""
        header = {
            "v": 1,
            "topic": "sensors.temp",
            "codec": "zstd",
            "count": 100,
            "raw_len": 5000,
            "comp_len": 1250,
            "dict_id": "d:sensors.temp:abc123",
        }
        result = InputValidator.validate_header(header)
        assert result == header

    def test_reject_missing_fields(self):
        """Test that missing required fields are rejected."""
        header = {
            "v": 1,
            "topic": "test",
            # Missing codec, count, raw_len, comp_len
        }
        with pytest.raises(ValidationError, match="missing required fields"):
            InputValidator.validate_header(header)

    def test_reject_invalid_version(self):
        """Test that invalid protocol version is rejected."""
        header = {
            "v": 999,
            "topic": "test",
            "codec": "zstd",
            "count": 1,
            "raw_len": 100,
            "comp_len": 50,
        }
        with pytest.raises(ValidationError, match="Unsupported protocol version"):
            InputValidator.validate_header(header)

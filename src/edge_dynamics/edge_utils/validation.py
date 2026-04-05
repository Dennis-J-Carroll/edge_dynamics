# SPDX-License-Identifier: Apache-2.0
"""
Input validation and sanitization module for edge_dynamics.

Provides security-focused validation to prevent path traversal,
injection attacks, and malformed input.

Example:
    >>> from edge_utils.validation import InputValidator
    >>> validator = InputValidator()
    >>> topic = validator.validate_topic("sensors.temperature")
    >>> print(topic)
    sensors.temperature
"""

import re
from typing import Any, Dict, Optional


class ValidationError(ValueError):
    """Exception raised when validation fails."""

    pass


class InputValidator:
    """
    Input validator with security-focused checks.

    Validates topics, file paths, and message payloads to prevent
    common security issues like path traversal, injection, and DoS.
    """

    # Topic must be alphanumeric with dots, dashes, and underscores
    TOPIC_PATTERN = re.compile(r"^[a-zA-Z0-9._-]+$")
    MAX_TOPIC_LENGTH = 255
    MIN_TOPIC_LENGTH = 1

    # Message size limits
    MAX_MESSAGE_SIZE = 10 * 1024 * 1024  # 10 MB
    MAX_BATCH_SIZE = 100 * 1024 * 1024  # 100 MB

    # Dict ID pattern
    DICT_ID_PATTERN = re.compile(r"^[a-zA-Z0-9:_-]+$")
    MAX_DICT_ID_LENGTH = 128

    @classmethod
    def validate_topic(cls, topic: str) -> str:
        """
        Validate and sanitize topic name.

        Args:
            topic: Topic name to validate

        Returns:
            Validated topic name

        Raises:
            ValidationError: If topic is invalid

        Example:
            >>> InputValidator.validate_topic("sensors.temp")
            'sensors.temp'
            >>> InputValidator.validate_topic("../../../etc/passwd")
            ValidationError: Path traversal detected in topic
        """
        if not topic:
            raise ValidationError("Topic cannot be empty")

        if not isinstance(topic, str):
            raise ValidationError(f"Topic must be a string, got {type(topic).__name__}")

        # Check length
        if len(topic) < cls.MIN_TOPIC_LENGTH:
            raise ValidationError(f"Topic must be at least {cls.MIN_TOPIC_LENGTH} character(s)")

        if len(topic) > cls.MAX_TOPIC_LENGTH:
            raise ValidationError(f"Topic exceeds maximum length of {cls.MAX_TOPIC_LENGTH} characters")

        # Check for path traversal
        if ".." in topic:
            raise ValidationError("Path traversal detected in topic")

        if topic.startswith("/") or topic.startswith("\\"):
            raise ValidationError("Topic cannot start with path separator")

        # Check pattern
        if not cls.TOPIC_PATTERN.match(topic):
            raise ValidationError(
                "Topic must contain only alphanumeric characters, dots, dashes, and underscores"
            )

        # Additional security checks
        if topic.endswith("."):
            raise ValidationError("Topic cannot end with a dot")

        if ".." in topic:
            raise ValidationError("Topic cannot contain consecutive dots")

        return topic

    @classmethod
    def validate_dict_id(cls, dict_id: str) -> str:
        """
        Validate dictionary ID.

        Args:
            dict_id: Dictionary identifier

        Returns:
            Validated dict_id

        Raises:
            ValidationError: If dict_id is invalid

        Example:
            >>> InputValidator.validate_dict_id("d:sensors.temp:abc123")
            'd:sensors.temp:abc123'
        """
        if not dict_id:
            return dict_id  # Empty dict_id is allowed (no compression)

        if not isinstance(dict_id, str):
            raise ValidationError(f"Dict ID must be a string, got {type(dict_id).__name__}")

        if len(dict_id) > cls.MAX_DICT_ID_LENGTH:
            raise ValidationError(f"Dict ID exceeds maximum length of {cls.MAX_DICT_ID_LENGTH}")

        if not cls.DICT_ID_PATTERN.match(dict_id):
            raise ValidationError("Dict ID contains invalid characters")

        return dict_id

    @classmethod
    def validate_message_size(cls, data: bytes) -> bytes:
        """
        Validate message size to prevent DoS.

        Args:
            data: Message data

        Returns:
            Validated data

        Raises:
            ValidationError: If message is too large

        Example:
            >>> InputValidator.validate_message_size(b"small message")
            b'small message'
        """
        if len(data) > cls.MAX_MESSAGE_SIZE:
            raise ValidationError(
                f"Message size {len(data)} exceeds maximum of {cls.MAX_MESSAGE_SIZE} bytes"
            )
        return data

    @classmethod
    def validate_batch_size(cls, data: bytes) -> bytes:
        """
        Validate batch size to prevent DoS.

        Args:
            data: Batch data

        Returns:
            Validated data

        Raises:
            ValidationError: If batch is too large
        """
        if len(data) > cls.MAX_BATCH_SIZE:
            raise ValidationError(f"Batch size {len(data)} exceeds maximum of {cls.MAX_BATCH_SIZE} bytes")
        return data

    @classmethod
    def validate_compression_level(cls, level: int) -> int:
        """
        Validate compression level.

        Args:
            level: Compression level (1-22 for zstd)

        Returns:
            Validated level

        Raises:
            ValidationError: If level is out of range
        """
        if not isinstance(level, int):
            raise ValidationError(f"Compression level must be an integer, got {type(level).__name__}")

        if not 1 <= level <= 22:
            raise ValidationError("Compression level must be between 1 and 22")

        return level

    @classmethod
    def sanitize_path(cls, path: str, base_dir: str) -> str:
        """
        Sanitize file path to prevent traversal attacks.

        Args:
            path: File path to sanitize
            base_dir: Base directory that path must be within

        Returns:
            Sanitized absolute path

        Raises:
            ValidationError: If path attempts to escape base_dir

        Example:
            >>> InputValidator.sanitize_path("data.jsonl", "/app/out")
            '/app/out/data.jsonl'
            >>> InputValidator.sanitize_path("../../etc/passwd", "/app/out")
            ValidationError: Path attempts to escape base directory
        """
        import os.path

        # Resolve to absolute path
        abs_base = os.path.abspath(base_dir)
        abs_path = os.path.abspath(os.path.join(abs_base, path))

        # Check if resolved path is within base directory
        if not abs_path.startswith(abs_base + os.sep):
            raise ValidationError(f"Path attempts to escape base directory: {path}")

        return abs_path

    @classmethod
    def validate_port(cls, port: int) -> int:
        """
        Validate port number.

        Args:
            port: Port number

        Returns:
            Validated port

        Raises:
            ValidationError: If port is invalid
        """
        if not isinstance(port, int):
            raise ValidationError(f"Port must be an integer, got {type(port).__name__}")

        if not 1 <= port <= 65535:
            raise ValidationError("Port must be between 1 and 65535")

        return port

    @classmethod
    def validate_header(cls, header: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate message header structure.

        Args:
            header: Header dictionary

        Returns:
            Validated header

        Raises:
            ValidationError: If header is malformed
        """
        if not isinstance(header, dict):
            raise ValidationError(f"Header must be a dictionary, got {type(header).__name__}")

        # Required fields
        required_fields = {"v", "topic", "codec", "count", "raw_len", "comp_len"}
        missing_fields = required_fields - set(header.keys())
        if missing_fields:
            raise ValidationError(f"Header missing required fields: {missing_fields}")

        # Validate protocol version
        if header["v"] != 1:
            raise ValidationError(f"Unsupported protocol version: {header['v']}")

        # Validate topic
        cls.validate_topic(header["topic"])

        # Validate counts
        if not isinstance(header["count"], int) or header["count"] < 1:
            raise ValidationError("Message count must be a positive integer")

        if not isinstance(header["raw_len"], int) or header["raw_len"] < 0:
            raise ValidationError("Raw length must be a non-negative integer")

        if not isinstance(header["comp_len"], int) or header["comp_len"] < 0:
            raise ValidationError("Compressed length must be a non-negative integer")

        # Validate dict_id if present
        if "dict_id" in header:
            cls.validate_dict_id(header["dict_id"])

        return header

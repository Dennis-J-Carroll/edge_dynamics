# SPDX-License-Identifier: Apache-2.0
"""Tests for edge_agent module."""

import json
import pytest
from edge_agent import normalize_message


class TestNormalizeMessage:
    """Tests for normalize_message function."""

    def test_normalize_removes_trace_id(self):
        """Test that X-Amzn-Trace-Id is removed from headers."""
        msg = {
            "headers": {
                "Accept": "*/*",
                "X-Amzn-Trace-Id": "Root=1-12345-67890",
            },
            "data": "test"
        }
        result = normalize_message(msg)
        result_dict = json.loads(result.decode())
        assert "X-Amzn-Trace-Id" not in result_dict["headers"]
        assert result_dict["headers"]["Accept"] == "*/*"

    def test_normalize_handles_missing_headers(self):
        """Test that normalize_message handles messages without headers."""
        msg = {"data": "test"}
        result = normalize_message(msg)
        result_dict = json.loads(result.decode())
        assert result_dict == {"data": "test"}

    def test_normalize_produces_compact_json(self):
        """Test that normalized JSON has no spaces."""
        msg = {"key": "value", "nested": {"a": 1, "b": 2}}
        result = normalize_message(msg)
        # Compact JSON should not have spaces after separators
        assert b" " not in result
        assert b'{"key":"value","nested":{"a":1,"b":2}}' == result or \
               b'{"nested":{"a":1,"b":2},"key":"value"}' == result

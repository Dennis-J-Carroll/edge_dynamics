# SPDX-License-Identifier: Apache-2.0
"""Tests for edge_utils.metrics module."""

import pytest
from edge_utils.metrics import Metrics, MetricsCollector, TopicMetrics


class TestMetrics:
    """Tests for Metrics class."""

    def test_record_batch(self):
        """Test recording a batch."""
        metrics = Metrics()
        metrics.record_batch(100, 5000, 1250, 15.5)

        assert metrics.messages_processed == 100
        assert metrics.bytes_in == 5000
        assert metrics.bytes_out == 1250
        assert metrics.flush_count == 1

    def test_compression_ratio(self):
        """Test compression ratio calculation."""
        metrics = Metrics()
        metrics.record_batch(100, 5000, 1250, 15.5)

        assert metrics.compression_ratio == 0.25  # 1250/5000

    def test_avg_flush_duration(self):
        """Test average flush duration calculation."""
        metrics = Metrics()
        metrics.record_batch(100, 5000, 1250, 15.5)
        metrics.record_batch(100, 5000, 1250, 20.5)

        assert metrics.avg_flush_duration_ms == 18.0  # (15.5 + 20.5) / 2

    def test_record_errors(self):
        """Test error recording."""
        metrics = Metrics()
        metrics.record_compression_error()
        metrics.record_network_error()

        assert metrics.compression_errors == 1
        assert metrics.network_errors == 1

    def test_reset(self):
        """Test metrics reset."""
        metrics = Metrics()
        metrics.record_batch(100, 5000, 1250, 15.5)
        metrics.reset()

        assert metrics.messages_processed == 0
        assert metrics.bytes_in == 0
        assert metrics.flush_count == 0


class TestMetricsCollector:
    """Tests for MetricsCollector class."""

    def test_record_batch_per_topic(self):
        """Test recording batches for different topics."""
        collector = MetricsCollector()
        collector.record_batch("topic1", 100, 5000, 1250, 15.0)
        collector.record_batch("topic2", 50, 3000, 900, 10.0)

        stats = collector.get_stats()
        assert stats["topic_count"] == 2
        assert stats["overall"]["messages_processed"] == 150
        assert len(stats["topics"]) == 2

    def test_get_topic_stats(self):
        """Test getting stats for a specific topic."""
        collector = MetricsCollector()
        collector.record_batch("sensors.temp", 100, 5000, 1250, 15.0)

        stats = collector.get_topic_stats("sensors.temp")
        assert stats is not None
        assert stats["topic"] == "sensors.temp"
        assert stats["messages_processed"] == 100
        assert stats["compression_ratio"] == 0.25

    def test_get_missing_topic_stats(self):
        """Test getting stats for non-existent topic."""
        collector = MetricsCollector()
        stats = collector.get_topic_stats("nonexistent")
        assert stats is None

    def test_export_csv_row(self):
        """Test CSV export."""
        collector = MetricsCollector()
        collector.record_batch("test.topic", 100, 5000, 1250, 15.0)

        csv_row = collector.export_csv_row("test.topic")
        assert csv_row is not None
        assert "test.topic" in csv_row
        assert "100" in csv_row
        assert "0.2500" in csv_row  # compression ratio

    def test_record_errors_per_topic(self):
        """Test recording errors for specific topics."""
        collector = MetricsCollector()
        collector.record_batch("topic1", 100, 5000, 1250, 15.0)
        collector.record_compression_error("topic1")
        collector.record_network_error("topic1")

        stats = collector.get_topic_stats("topic1")
        assert stats["compression_errors"] == 1
        assert stats["network_errors"] == 1


class TestTopicMetrics:
    """Tests for TopicMetrics dataclass."""

    def test_compression_ratio_calculation(self):
        """Test compression ratio property."""
        metrics = TopicMetrics(topic="test")
        metrics.bytes_in = 1000
        metrics.bytes_out = 250

        assert metrics.compression_ratio == 0.25

    def test_compression_ratio_zero_division(self):
        """Test compression ratio with zero input."""
        metrics = TopicMetrics(topic="test")
        assert metrics.compression_ratio == 0.0

    def test_to_dict(self):
        """Test conversion to dictionary."""
        metrics = TopicMetrics(
            topic="test",
            messages_processed=100,
            bytes_in=5000,
            bytes_out=1250,
        )

        data = metrics.to_dict()
        assert data["topic"] == "test"
        assert data["messages_processed"] == 100
        assert data["compression_ratio"] == 0.25

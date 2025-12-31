# SPDX-License-Identifier: Apache-2.0
"""
Metrics collection and reporting module for edge_dynamics.

Provides thread-safe metrics collection with aggregation and
export capabilities.

Example:
    >>> from edge_utils.metrics import MetricsCollector
    >>> metrics = MetricsCollector()
    >>> metrics.record_batch("sensors.temp", 100, 5000, 1250, 0.15)
    >>> stats = metrics.get_stats()
    >>> print(f"Compression ratio: {stats['overall_compression_ratio']:.2%}")
"""

import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class TopicMetrics:
    """Metrics for a single topic."""

    topic: str
    messages_processed: int = 0
    bytes_in: int = 0
    bytes_out: int = 0
    flush_count: int = 0
    total_flush_duration_ms: float = 0.0
    compression_errors: int = 0
    network_errors: int = 0
    last_flush_timestamp: Optional[float] = None

    @property
    def compression_ratio(self) -> float:
        """Calculate compression ratio (compressed/original)."""
        if self.bytes_in == 0:
            return 0.0
        return self.bytes_out / self.bytes_in

    @property
    def avg_flush_duration_ms(self) -> float:
        """Calculate average flush duration in milliseconds."""
        if self.flush_count == 0:
            return 0.0
        return self.total_flush_duration_ms / self.flush_count

    @property
    def throughput_mbps(self) -> float:
        """Calculate throughput in MB/s."""
        if self.total_flush_duration_ms == 0:
            return 0.0
        return (self.bytes_in / 1024 / 1024) / (self.total_flush_duration_ms / 1000)

    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            "topic": self.topic,
            "messages_processed": self.messages_processed,
            "bytes_in": self.bytes_in,
            "bytes_out": self.bytes_out,
            "compression_ratio": self.compression_ratio,
            "flush_count": self.flush_count,
            "avg_flush_duration_ms": self.avg_flush_duration_ms,
            "throughput_mbps": self.throughput_mbps,
            "compression_errors": self.compression_errors,
            "network_errors": self.network_errors,
            "last_flush_timestamp": self.last_flush_timestamp,
        }


@dataclass
class Metrics:
    """
    Overall metrics aggregator.

    Thread-safe metrics collection for the entire edge agent.
    """

    messages_processed: int = 0
    bytes_in: int = 0
    bytes_out: int = 0
    flush_count: int = 0
    total_flush_duration_ms: float = 0.0
    compression_errors: int = 0
    network_errors: int = 0
    start_time: float = field(default_factory=time.time)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    @property
    def compression_ratio(self) -> float:
        """Calculate overall compression ratio."""
        if self.bytes_in == 0:
            return 0.0
        return self.bytes_out / self.bytes_in

    @property
    def avg_flush_duration_ms(self) -> float:
        """Calculate average flush duration."""
        if self.flush_count == 0:
            return 0.0
        return self.total_flush_duration_ms / self.flush_count

    @property
    def uptime_seconds(self) -> float:
        """Calculate uptime in seconds."""
        return time.time() - self.start_time

    @property
    def throughput_mbps(self) -> float:
        """Calculate throughput in MB/s."""
        if self.uptime_seconds == 0:
            return 0.0
        return (self.bytes_in / 1024 / 1024) / self.uptime_seconds

    @property
    def messages_per_second(self) -> float:
        """Calculate messages per second."""
        if self.uptime_seconds == 0:
            return 0.0
        return self.messages_processed / self.uptime_seconds

    def record_batch(
        self, message_count: int, raw_bytes: int, compressed_bytes: int, duration_ms: float
    ) -> None:
        """
        Record a batch flush.

        Args:
            message_count: Number of messages in batch
            raw_bytes: Original (uncompressed) size
            compressed_bytes: Compressed size
            duration_ms: Time taken to compress and send
        """
        with self._lock:
            self.messages_processed += message_count
            self.bytes_in += raw_bytes
            self.bytes_out += compressed_bytes
            self.flush_count += 1
            self.total_flush_duration_ms += duration_ms

    def record_compression_error(self) -> None:
        """Record a compression error."""
        with self._lock:
            self.compression_errors += 1

    def record_network_error(self) -> None:
        """Record a network error."""
        with self._lock:
            self.network_errors += 1

    def get_stats(self) -> Dict:
        """Get current metrics as dictionary."""
        with self._lock:
            return {
                "messages_processed": self.messages_processed,
                "bytes_in": self.bytes_in,
                "bytes_out": self.bytes_out,
                "compression_ratio": self.compression_ratio,
                "flush_count": self.flush_count,
                "avg_flush_duration_ms": self.avg_flush_duration_ms,
                "uptime_seconds": self.uptime_seconds,
                "throughput_mbps": self.throughput_mbps,
                "messages_per_second": self.messages_per_second,
                "compression_errors": self.compression_errors,
                "network_errors": self.network_errors,
            }

    def reset(self) -> None:
        """Reset all metrics."""
        with self._lock:
            self.messages_processed = 0
            self.bytes_in = 0
            self.bytes_out = 0
            self.flush_count = 0
            self.total_flush_duration_ms = 0.0
            self.compression_errors = 0
            self.network_errors = 0
            self.start_time = time.time()


class MetricsCollector:
    """
    Per-topic metrics collector.

    Maintains separate metrics for each topic while also aggregating
    overall statistics.
    """

    def __init__(self):
        """Initialize metrics collector."""
        self.overall = Metrics()
        self.topics: Dict[str, TopicMetrics] = defaultdict(lambda: TopicMetrics(topic=""))
        self._lock = threading.Lock()

    def record_batch(
        self, topic: str, message_count: int, raw_bytes: int, compressed_bytes: int, duration_ms: float
    ) -> None:
        """
        Record a batch flush for a specific topic.

        Args:
            topic: Topic name
            message_count: Number of messages
            raw_bytes: Original size
            compressed_bytes: Compressed size
            duration_ms: Flush duration

        Example:
            >>> collector = MetricsCollector()
            >>> collector.record_batch("sensors.temp", 100, 5000, 1250, 15.5)
        """
        with self._lock:
            # Update topic metrics
            if topic not in self.topics:
                self.topics[topic] = TopicMetrics(topic=topic)

            topic_metrics = self.topics[topic]
            topic_metrics.messages_processed += message_count
            topic_metrics.bytes_in += raw_bytes
            topic_metrics.bytes_out += compressed_bytes
            topic_metrics.flush_count += 1
            topic_metrics.total_flush_duration_ms += duration_ms
            topic_metrics.last_flush_timestamp = time.time()

            # Update overall metrics
            self.overall.record_batch(message_count, raw_bytes, compressed_bytes, duration_ms)

    def record_compression_error(self, topic: str) -> None:
        """Record a compression error for a topic."""
        with self._lock:
            if topic in self.topics:
                self.topics[topic].compression_errors += 1
            self.overall.record_compression_error()

    def record_network_error(self, topic: str) -> None:
        """Record a network error for a topic."""
        with self._lock:
            if topic in self.topics:
                self.topics[topic].network_errors += 1
            self.overall.record_network_error()

    def get_topic_stats(self, topic: str) -> Optional[Dict]:
        """
        Get statistics for a specific topic.

        Args:
            topic: Topic name

        Returns:
            Dictionary of topic statistics or None if topic not found
        """
        with self._lock:
            if topic in self.topics:
                return self.topics[topic].to_dict()
            return None

    def get_all_topics_stats(self) -> List[Dict]:
        """
        Get statistics for all topics.

        Returns:
            List of topic statistics dictionaries
        """
        with self._lock:
            return [metrics.to_dict() for metrics in self.topics.values()]

    def get_stats(self) -> Dict:
        """
        Get overall statistics plus per-topic breakdown.

        Returns:
            Complete metrics dictionary

        Example:
            >>> collector = MetricsCollector()
            >>> collector.record_batch("sensors.temp", 100, 5000, 1250, 15.0)
            >>> stats = collector.get_stats()
            >>> print(stats['overall_compression_ratio'])
            0.25
        """
        with self._lock:
            return {
                "overall": self.overall.get_stats(),
                "topics": [metrics.to_dict() for metrics in self.topics.values()],
                "topic_count": len(self.topics),
            }

    def reset(self) -> None:
        """Reset all metrics."""
        with self._lock:
            self.overall.reset()
            self.topics.clear()

    def export_csv_row(self, topic: str) -> Optional[str]:
        """
        Export topic metrics as CSV row.

        Args:
            topic: Topic name

        Returns:
            CSV-formatted string or None if topic not found

        Example:
            >>> collector = MetricsCollector()
            >>> row = collector.export_csv_row("sensors.temp")
            >>> print(row)
            sensors.temp,100,5000,1250,0.25,1,15.0
        """
        topic_stats = self.get_topic_stats(topic)
        if not topic_stats:
            return None

        return (
            f"{topic_stats['topic']},"
            f"{topic_stats['messages_processed']},"
            f"{topic_stats['bytes_in']},"
            f"{topic_stats['bytes_out']},"
            f"{topic_stats['compression_ratio']:.4f},"
            f"{topic_stats['flush_count']},"
            f"{topic_stats['avg_flush_duration_ms']:.2f}"
        )
# SPDX-License-Identifier: Apache-2.0
#!/usr/bin/env python3
"""
edge_agent_v2.py

Production-ready edge agent with comprehensive reliability features.

Improvements over v1:
- Structured JSON logging for observability
- Configuration management with environment variables
- Input validation for security
- Comprehensive metrics collection
- Circuit breaker for fault tolerance
- Connection pooling for performance
- Better error handling and recovery

This version integrates all edge_utils modules for production deployment.
"""

import json
import os
import struct
import threading
import time
from collections import defaultdict, deque
from typing import Any, Deque, Dict, Optional

import ujson  # type: ignore
import zstandard as zstd

# Import edge_utils
from edge_utils import (
    get_logger,
    get_settings,
    InputValidator,
    ValidationError,
    MetricsCollector,
    CircuitBreaker,
    CircuitBreakerError,
    ConnectionPool,
)

# Initialize utilities
logger = get_logger("edge_agent")
settings = get_settings()
validator = InputValidator()
metrics = MetricsCollector()


def load_dictionaries() -> Dict[str, dict]:
    """Load dictionary index and return a mapping of topic -> dict info."""
    index_path = os.path.join(settings.dict_dir, "dict_index.json")

    if not os.path.exists(index_path):
        logger.warning("dict_index_not_found", path=index_path)
        return {}

    try:
        with open(index_path, "r") as f:
            index = json.load(f)
        logger.info("dictionaries_loaded", count=len(index), path=index_path)
        return index
    except Exception as e:
        logger.error("failed_to_load_dictionaries", error=str(e), path=index_path)
        return {}


def build_compressors(index: Dict[str, dict]) -> Dict[str, dict]:
    """Build zstd compressors for each topic based on loaded dictionaries."""
    compressors: Dict[str, dict] = {}

    for topic, meta in index.items():
        try:
            dict_path = meta["path"]
            if not os.path.exists(dict_path):
                logger.warning("dictionary_file_not_found", topic=topic, path=dict_path)
                continue

            with open(dict_path, "rb") as f:
                zdict = zstd.ZstdDictionary(f.read())

            compressors[topic] = {
                "dict_id": meta["dict_id"],
                "compressor": zstd.ZstdCompressor(level=settings.compression_level, dict_data=zdict),
            }

            logger.debug(
                "compressor_built",
                topic=topic,
                dict_id=meta["dict_id"],
                size=len(zdict.as_bytes()),
            )

        except Exception as e:
            logger.error("failed_to_build_compressor", topic=topic, error=str(e))

    logger.info("compressors_ready", count=len(compressors))
    return compressors


def normalize_message(msg: dict) -> bytes:
    """Remove volatile headers and serialize message to compact JSON bytes."""
    headers = msg.get("headers")
    if isinstance(headers, dict):
        headers.pop("X-Amzn-Trace-Id", None)

    # Use ujson for faster serialization
    return ujson.dumps(msg, ensure_ascii=False, separators=(",", ":")).encode()


class EdgeAgent:
    """
    Production-ready edge agent with reliability features.

    Features:
    - Structured logging
    - Configuration management
    - Input validation
    - Metrics collection
    - Circuit breaker pattern
    - Connection pooling
    """

    def __init__(self) -> None:
        """Initialize edge agent with production features."""
        logger.info(
            "initializing_edge_agent",
            collector=f"{settings.collector_host}:{settings.collector_port}",
            batch_max=settings.batch_max,
            batch_ms=settings.batch_ms,
            compression_level=settings.compression_level,
        )

        # Load dictionaries
        self.dict_index = load_dictionaries()
        self.compressors = build_compressors(self.dict_index)

        # Message buffers
        self.buffers: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"q": deque(), "t0": None})
        self.lock = threading.Lock()

        # Connection pool for efficient socket reuse
        self.connection_pool = ConnectionPool(
            host=settings.collector_host,
            port=settings.collector_port,
            max_size=settings.max_workers or 10,
            timeout=settings.connection_timeout,
        )

        # Circuit breaker to prevent cascading failures
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=5, timeout=60.0, success_threshold=2, name="collector"
        )

        # Background flusher thread
        self.flusher_thread = threading.Thread(target=self._background_flusher, daemon=True)
        self.running = False

        logger.info("edge_agent_initialized", dict_count=len(self.compressors))

    def start(self) -> None:
        """Start the edge agent."""
        self.running = True
        self.flusher_thread.start()
        logger.info("edge_agent_started")

    def stop(self) -> None:
        """Stop the edge agent gracefully."""
        logger.info("stopping_edge_agent")
        self.running = False

        # Flush remaining messages
        with self.lock:
            topics = list(self.buffers.keys())

        for topic in topics:
            try:
                self._flush_if_needed(topic, force=True)
            except Exception as e:
                logger.error("flush_failed_during_shutdown", topic=topic, error=str(e))

        # Close connection pool
        self.connection_pool.close_all()

        logger.info("edge_agent_stopped")

    def enqueue(self, topic: str, msg: dict) -> None:
        """
        Normalize and add a message to the buffer for its topic.

        Args:
            topic: Topic name
            msg: Message dictionary

        Raises:
            ValidationError: If topic is invalid
        """
        # Validate topic
        try:
            topic = validator.validate_topic(topic)
        except ValidationError as e:
            logger.error("invalid_topic", topic=topic, error=str(e))
            raise

        # Normalize message
        try:
            data = normalize_message(msg)
        except Exception as e:
            logger.error("normalization_failed", topic=topic, error=str(e))
            raise

        # Add to buffer
        with self.lock:
            buf = self.buffers[topic]
            q: Deque[bytes] = buf["q"]  # type: ignore[assignment]

            if not q:
                buf["t0"] = time.time() * 1000  # record first message time in ms

            q.append(data)

        # Attempt immediate flush if threshold reached
        self._flush_if_needed(topic)

    def _flush_if_needed(self, topic: str, force: bool = False) -> None:
        """Check if buffer should be flushed and flush if needed."""
        with self.lock:
            buf = self.buffers[topic]
            q: Deque[bytes] = buf["q"]  # type: ignore[assignment]

            if not q:
                return

            t0 = buf["t0"]
            age = (time.time() * 1000 - t0) if t0 else 0

            should_flush = force or len(q) >= settings.batch_max or age >= settings.batch_ms

            if should_flush:
                # Detach messages for flushing
                msgs = list(q)
                buf["q"] = deque()
                buf["t0"] = None
            else:
                return

        # Release lock while compressing and sending
        if msgs:
            self._flush_batch(topic, msgs)

    def _background_flusher(self) -> None:
        """Periodic flusher; wakes up periodically to flush aged batches."""
        logger.info("background_flusher_started")

        while self.running:
            time.sleep(settings.batch_ms / 1000.0)

            topics = []
            with self.lock:
                for topic, buf in self.buffers.items():
                    q: Deque[bytes] = buf["q"]  # type: ignore[assignment]

                    if not q:
                        continue

                    t0 = buf["t0"]
                    age = (time.time() * 1000 - t0) if t0 else 0

                    if age >= settings.batch_ms:
                        msgs = list(q)
                        buf["q"] = deque()
                        buf["t0"] = None
                        topics.append((topic, msgs))

            # Flush outside lock
            for topic, msgs in topics:
                self._flush_batch(topic, msgs)

        logger.info("background_flusher_stopped")

    def _flush_batch(self, topic: str, msgs: list) -> None:
        """Flush a batch of messages for a topic."""
        flush_start = time.time()

        # Combine messages
        raw = b"\n".join(msgs) + b"\n"
        raw_len = len(raw)

        # Compress
        try:
            if topic not in self.compressors:
                # No compressor for this topic, send uncompressed
                comp_payload = raw
                dict_id = ""
                logger.warning("no_compressor_for_topic", topic=topic)
            else:
                comp = self.compressors[topic]["compressor"]
                dict_id = self.compressors[topic]["dict_id"]
                comp_payload = comp.compress(raw)

        except Exception as e:
            logger.error("compression_failed", topic=topic, error=str(e))
            metrics.record_compression_error(topic)
            return

        # Build header
        header = {
            "v": 1,
            "topic": topic,
            "codec": settings.compression_codec,
            "level": settings.compression_level,
            "dict_id": dict_id,
            "schema_id": "s1",
            "count": len(msgs),
            "raw_len": raw_len,
            "comp_len": len(comp_payload),
            "t0": flush_start,
            "t1": time.time(),
        }

        hdr_bytes = json.dumps(header, separators=(",", ":")).encode()
        frame = struct.pack("!I", len(hdr_bytes)) + hdr_bytes + comp_payload

        # Send to collector with circuit breaker and connection pooling
        try:
            self._send_with_retry(frame)

            # Record successful flush
            duration_ms = (time.time() - flush_start) * 1000
            metrics.record_batch(topic, len(msgs), raw_len, len(comp_payload), duration_ms)

            ratio = len(comp_payload) / max(raw_len, 1)
            logger.info(
                "batch_flushed",
                topic=topic,
                count=len(msgs),
                raw_bytes=raw_len,
                compressed_bytes=len(comp_payload),
                ratio=f"{ratio:.2%}",
                duration_ms=f"{duration_ms:.1f}",
            )

        except CircuitBreakerError as e:
            logger.warning("circuit_breaker_open", topic=topic, error=str(e))
            metrics.record_network_error(topic)

        except Exception as e:
            logger.error("send_failed", topic=topic, error=str(e))
            metrics.record_network_error(topic)

    def _send_with_retry(self, frame: bytes) -> None:
        """Send frame with circuit breaker protection."""

        def send():
            with self.connection_pool.get_connection() as conn:
                conn.sendall(frame)

        # Use circuit breaker
        self.circuit_breaker.call(send)

    def get_stats(self) -> dict:
        """Get agent statistics."""
        return {
            "metrics": metrics.get_stats(),
            "circuit_breaker": self.circuit_breaker.get_stats(),
            "connection_pool": self.connection_pool.get_stats(),
        }


def synth_feed(agent: EdgeAgent) -> None:
    """Generate synthetic file telemetry messages for testing."""
    import random
    import string

    topics = ["files.txt", "files.csv", "files.json"]

    logger.info("synthetic_feed_started", topics=topics)

    while agent.running:
        for topic in topics:
            # Create a fake message with file metadata
            filename = "".join(random.choices(string.ascii_lowercase, k=8)) + "." + topic.split(".")[-1]

            msg = {
                "file_type": topic.split(".")[-1],
                "path": f"/var/log/{topic}/{filename}",
                "size": random.randint(10, 10_000),
                "checksum": "".join(random.choices("abcdef0123456789", k=8)),
                "headers": {
                    "Accept": "*/*",
                    "User-Agent": "edge-agent/2.0",
                    "X-Amzn-Trace-Id": f"Root={random.randint(1,10**8)}-{random.randint(10**16,10**17-1)}",
                },
            }

            try:
                agent.enqueue(topic, msg)
            except ValidationError as e:
                logger.error("validation_error", topic=topic, error=str(e))

            time.sleep(0.005)  # 200 msgs/s aggregate


def main() -> None:
    """Main entry point."""
    import signal

    agent = EdgeAgent()
    agent.start()

    # Handle graceful shutdown
    def shutdown_handler(signum, frame):
        logger.info("shutdown_signal_received", signal=signum)
        agent.stop()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # Run synthetic feed
    try:
        synth_feed(agent)
    except KeyboardInterrupt:
        logger.info("keyboard_interrupt")
        agent.stop()


if __name__ == "__main__":
    main()

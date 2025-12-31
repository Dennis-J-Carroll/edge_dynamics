# SPDX-License-Identifier: Apache-2.0
#!/usr/bin/env python3
"""
collector_server_v2.py

Production-ready TCP collector for compressed batches from edge agents.

Improvements over v1:
- Structured JSON logging
- Configuration management with environment variables
- Input validation for headers
- Comprehensive metrics collection
- Better error handling
- Graceful shutdown
- Path sanitization for security

This version integrates all edge_utils modules for production deployment.
"""

import csv
import json
import os
import signal
import socket
import struct
import threading
import time
from typing import Dict, Tuple

import zstandard as zstd

# Import edge_utils
from edge_utils import (
    get_logger,
    get_settings,
    InputValidator,
    ValidationError,
    MetricsCollector,
)

# Initialize utilities
logger = get_logger("collector")
settings = get_settings()
validator = InputValidator()
metrics = MetricsCollector()


def load_dictionaries() -> Tuple[Dict[str, dict], Dict[str, zstd.ZstdDecompressor]]:
    """Load dictionaries and return mapping of dict_id to decompressors."""
    index_path = os.path.join(settings.dict_dir, "dict_index.json")

    if not os.path.exists(index_path):
        logger.warning("dict_index_not_found", path=index_path)
        return {}, {}

    try:
        with open(index_path, "r") as f:
            index = json.load(f)

        dec: Dict[str, zstd.ZstdDecompressor] = {}
        for topic, meta in index.items():
            path = meta["path"]

            if not os.path.exists(path):
                logger.warning("dictionary_file_not_found", topic=topic, path=path)
                continue

            with open(path, "rb") as fd:
                zd = zstd.ZstdDictionary(fd.read())
            dec[meta["dict_id"]] = zstd.ZstdDecompressor(dict_data=zd)

            logger.debug("decompressor_loaded", topic=topic, dict_id=meta["dict_id"])

        logger.info("dictionaries_loaded", count=len(dec), path=index_path)
        return index, dec

    except Exception as e:
        logger.error("failed_to_load_dictionaries", error=str(e), path=index_path)
        return {}, {}


def recvall(sock: socket.socket, n: int) -> bytes:
    """
    Receive exactly n bytes from socket.

    Args:
        sock: Socket to receive from
        n: Number of bytes to receive

    Returns:
        Received bytes

    Raises:
        ConnectionError: If socket closes before receiving all bytes
        ValidationError: If requested size is too large
    """
    # Validate size to prevent DoS
    if n > 100 * 1024 * 1024:  # 100 MB max
        raise ValidationError(f"Requested receive size too large: {n} bytes")

    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(min(n - len(buf), 8192))
        if not chunk:
            raise ConnectionError("Socket closed before receiving expected bytes")
        buf.extend(chunk)

    return bytes(buf)


class CollectorServer:
    """
    Production-ready collector server.

    Features:
    - Structured logging
    - Configuration management
    - Input validation
    - Metrics collection
    - Graceful shutdown
    """

    def __init__(self):
        """Initialize collector server."""
        logger.info(
            "initializing_collector",
            host=settings.server_host,
            port=settings.server_port,
            out_dir=settings.out_dir,
        )

        # Ensure output directory exists
        os.makedirs(settings.out_dir, exist_ok=True)

        # Initialize metrics CSV
        self._init_metrics_csv()

        # Load dictionaries
        self.dict_index, self.decompressors = load_dictionaries()

        # Server state
        self.running = False
        self.server_socket: Optional[socket.socket] = None

        logger.info("collector_initialized", dict_count=len(self.decompressors))

    def _init_metrics_csv(self) -> None:
        """Initialize metrics CSV file with header."""
        if not os.path.exists(settings.metrics_file):
            try:
                with open(settings.metrics_file, "w", newline="") as f:
                    csv.writer(f).writerow([
                        "timestamp",
                        "topic",
                        "count",
                        "raw_bytes",
                        "compressed_bytes",
                        "ratio",
                        "dict_id",
                    ])
                logger.info("metrics_csv_created", path=settings.metrics_file)
            except Exception as e:
                logger.error("failed_to_create_metrics_csv", error=str(e))

    def start(self) -> None:
        """Start the collector server."""
        logger.info("starting_collector")

        try:
            self.server_socket = socket.create_server(
                (settings.server_host, settings.server_port), reuse_port=True
            )
            self.running = True

            logger.info(
                "collector_listening",
                host=settings.server_host,
                port=settings.server_port,
            )

            while self.running:
                try:
                    # Accept connection
                    conn, addr = self.server_socket.accept()
                    logger.debug("connection_accepted", client=f"{addr[0]}:{addr[1]}")

                    # Handle in separate thread
                    thread = threading.Thread(target=self._handle_connection, args=(conn, addr))
                    thread.daemon = True
                    thread.start()

                except Exception as e:
                    if self.running:
                        logger.error("accept_error", error=str(e))

        except Exception as e:
            logger.error("server_error", error=str(e))
            raise
        finally:
            self.stop()

    def stop(self) -> None:
        """Stop the collector server gracefully."""
        logger.info("stopping_collector")
        self.running = False

        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception as e:
                logger.error("error_closing_server_socket", error=str(e))

        logger.info("collector_stopped")

    def _handle_connection(self, conn: socket.socket, addr: Tuple[str, int]) -> None:
        """Handle a single client connection."""
        start_time = time.time()

        try:
            with conn:
                # Read header length
                header_len_bytes = recvall(conn, 4)
                header_len = struct.unpack("!I", header_len_bytes)[0]

                # Validate header length
                if header_len > 10 * 1024:  # 10 KB max header
                    raise ValidationError(f"Header too large: {header_len} bytes")

                # Read header
                header_data = recvall(conn, header_len)
                header = json.loads(header_data)

                # Validate header structure
                header = validator.validate_header(header)

                # Read compressed payload
                compressed_len = header["comp_len"]
                comp_payload = recvall(conn, compressed_len)

                # Decompress
                dict_id = header.get("dict_id", "")

                if dict_id and dict_id not in self.decompressors:
                    logger.warning("unknown_dict_id", dict_id=dict_id)
                    # Try to decompress without dictionary
                    decompressor = zstd.ZstdDecompressor()
                else:
                    decompressor = self.decompressors.get(dict_id, zstd.ZstdDecompressor())

                decompressed = decompressor.decompress(comp_payload)

                # Validate topic
                topic = validator.validate_topic(header["topic"])

                # Write decompressed data
                self._write_output(topic, decompressed)

                # Record metrics
                self._record_metrics(header)

                # Log success
                duration_ms = (time.time() - start_time) * 1000
                ratio = header["comp_len"] / max(header["raw_len"], 1)

                logger.info(
                    "batch_received",
                    topic=topic,
                    count=header["count"],
                    raw_bytes=header["raw_len"],
                    compressed_bytes=header["comp_len"],
                    ratio=f"{ratio:.2%}",
                    duration_ms=f"{duration_ms:.1f}",
                    client=f"{addr[0]}:{addr[1]}",
                )

        except ValidationError as e:
            logger.error("validation_error", error=str(e), client=f"{addr[0]}:{addr[1]}")

        except Exception as e:
            logger.error("connection_error", error=str(e), client=f"{addr[0]}:{addr[1]}")

    def _write_output(self, topic: str, data: bytes) -> None:
        """Write decompressed data to output file."""
        try:
            # Sanitize filename
            safe_filename = f"{topic}.jsonl"
            out_path = validator.sanitize_path(safe_filename, settings.out_dir)

            with open(out_path, "ab") as f:
                f.write(data)

            logger.debug("data_written", topic=topic, path=out_path, size=len(data))

        except Exception as e:
            logger.error("write_error", topic=topic, error=str(e))
            raise

    def _record_metrics(self, header: dict) -> None:
        """Record metrics for a received batch."""
        try:
            # Record in metrics collector
            metrics.record_batch(
                topic=header["topic"],
                message_count=header["count"],
                raw_bytes=header["raw_len"],
                compressed_bytes=header["comp_len"],
                duration_ms=0,  # We don't track decompression time separately
            )

            # Append to CSV
            with open(settings.metrics_file, "a", newline="") as f:
                csv.writer(f).writerow([
                    time.time(),
                    header["topic"],
                    header["count"],
                    header["raw_len"],
                    header["comp_len"],
                    header["comp_len"] / max(header["raw_len"], 1),
                    header.get("dict_id", ""),
                ])

        except Exception as e:
            logger.error("metrics_error", error=str(e))

    def get_stats(self) -> dict:
        """Get collector statistics."""
        return {
            "metrics": metrics.get_stats(),
            "running": self.running,
            "dict_count": len(self.decompressors),
        }


def main() -> None:
    """Main entry point."""
    collector = CollectorServer()

    # Handle graceful shutdown
    def shutdown_handler(signum, frame):
        logger.info("shutdown_signal_received", signal=signum)
        collector.stop()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # Start server
    try:
        collector.start()
    except KeyboardInterrupt:
        logger.info("keyboard_interrupt")
        collector.stop()


if __name__ == "__main__":
    main()

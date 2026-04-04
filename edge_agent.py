#!/usr/bin/env python3
"""
edge_agent.py

This script implements a simple edge agent that batches messages by topic,
normalizes them to stable JSON (removing volatile headers), compresses
each batch using zstandard with a per‑topic dictionary, and sends the
compressed frame to a remote collector over TCP.

The agent maintains a per‑topic buffer and flushes when either the
number of queued messages reaches BATCH_MAX or the age of the oldest
message exceeds BATCH_MS milliseconds.

The payload protocol consists of:
    4 bytes: big‑endian integer length of the JSON header
    N bytes: compact JSON header with metadata
    M bytes: compressed payload

The header includes fields like topic, dict_id, count, raw_len,
comp_len and timestamps. See code below for details.

An example synthetic feeder is provided for testing, which generates
fake file telemetry messages across three topics (files.txt, files.csv,
files.json). Replace this with your own producer in practice.
"""

import json
import os
import socket
import struct
import threading
import time
from collections import defaultdict, deque
from typing import Any, Deque, Dict, List, Optional, Union

import ujson  # type: ignore
import zstandard as zstd
import psutil
from http.server import BaseHTTPRequestHandler, HTTPServer

from config import get_settings
from structured_logging import get_logger
from circuit_breaker import CircuitBreaker, CircuitState
from disk_buffer import DiskBuffer

# Initialize configuration and logging
settings = get_settings()
logger = get_logger("edge_agent")

# Initialize circuit breaker for collector connection
breaker = CircuitBreaker(
    failure_threshold=5,
    timeout=5.0,
    name="collector_connection"
)

class HealthHandler(BaseHTTPRequestHandler):
    """Simple HTTP handler for health checks."""
    agent: Any = None
    
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            
            health = {
                "status": "healthy",
                "timestamp": time.time(),
                "checks": {
                    "collector_reachable": self._check_collector(),
                    "dictionaries_loaded": len(self.agent.compressors) > 0,
                    "memory_usage_percent": psutil.virtual_memory().percent,
                    "circuit_breaker_state": breaker.state.value,
                    "disk_buffer_count": self.agent.disk_buffer.get_count() if self.agent.disk_buffer else 0
                },
                "metrics": self.agent.metrics.get_stats()
            }
            self.wfile.write(json.dumps(health).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def _check_collector(self):
        try:
            with socket.create_connection((settings.collector_host, settings.collector_port), timeout=1):
                return True
        except:
            return False

    def log_message(self, format, *args):
        pass

class Metrics:
    """Metrics collection for the edge agent."""
    def __init__(self):
        self.messages_processed = 0
        self.bytes_in = 0
        self.bytes_out = 0
        self.compression_ratio = 0.0
        self.batch_count = 0
        self.lock = threading.Lock()

    def record_batch(self, message_count: int, raw_bytes: int, compressed_bytes: int):
        with self.lock:
            self.messages_processed += message_count
            self.bytes_in += raw_bytes
            self.bytes_out += compressed_bytes
            self.batch_count += 1
            if self.bytes_in > 0:
                self.compression_ratio = self.bytes_out / self.bytes_in

    def get_stats(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "messages_processed": self.messages_processed,
                "bytes_in": self.bytes_in,
                "bytes_out": self.bytes_out,
                "compression_ratio": self.compression_ratio,
                "batch_count": self.batch_count
            }

def load_dictionaries() -> Dict[str, dict]:
    """Load dictionary index and return a mapping of topic -> dict info."""
    index_path = os.path.join(settings.dict_dir, "dict_index.json")
    try:
        with open(index_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning("dictionary_index_not_found", path=index_path)
        return {}

def build_compressors(index: Dict[str, dict]) -> Dict[str, dict]:
    """Build zstd compressors for each topic based on loaded dictionaries."""
    compressors: Dict[str, dict] = {}
    for topic, meta in index.items():
        try:
            with open(meta["path"], "rb") as f:
                zdict = zstd.ZstdCompressionDict(f.read())
            compressors[topic] = {
                "dict_id": meta["dict_id"],
                "compressor": zstd.ZstdCompressor(level=settings.compression_level, dict_data=zdict),
            }
        except Exception as e:
            logger.error("failed_to_build_compressor", topic=topic, error=e)
    return compressors

def normalize_message(msg: dict) -> bytes:
    """Remove volatile headers and serialize message to compact JSON bytes."""
    headers = msg.get("headers")
    if isinstance(headers, dict):
        headers.pop("X-Amzn-Trace-Id", None)
    # Use ujson for faster serialization; ensure compact form with separators
    return ujson.dumps(msg, ensure_ascii=False, separators=(",", ":")).encode()

class EdgeAgent:
    def __init__(self, enable_flush: bool = True, enable_metrics: bool = True, enable_health: bool = True) -> None:
        self.dict_index = load_dictionaries()
        self.compressors = build_compressors(self.dict_index)
        self.buffers: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {"q": deque(), "t0": None}
        )
        self.lock = threading.Lock()
        
        self.enable_flush = enable_flush
        self.enable_metrics = enable_metrics
        self.enable_health = enable_health
        
        self.flusher_thread = threading.Thread(target=self._background_flusher, daemon=True)
        self.metrics_thread = threading.Thread(target=self._metrics_reporter, daemon=True)
        self.health_thread = threading.Thread(target=self._health_server, daemon=True)
        self.recovery_thread = threading.Thread(target=self._recovery_loop, daemon=True)
        
        self._sock: Optional[socket.socket] = None
        self.metrics = Metrics()
        
        self.disk_buffer = None
        if settings.disk_buffer_enabled:
            self.disk_buffer = DiskBuffer(
                db_path=settings.disk_buffer_path,
                max_mb=settings.disk_buffer_max_mb
            )
            
        logger.info("edge_agent_initialized", 
                    collector=f"{settings.collector_host}:{settings.collector_port}",
                    batch_max=settings.batch_max,
                    batch_ms=settings.batch_ms,
                    disk_buffer=settings.disk_buffer_enabled)

    def start(self) -> None:
        if self.enable_flush:
            self.flusher_thread.start()
        if self.enable_metrics:
            self.metrics_thread.start()
        if self.enable_health:
            self.health_thread.start()
        if self.enable_flush and settings.disk_buffer_enabled:
            self.recovery_thread.start()
        logger.info("edge_agent_started")

    def _recovery_loop(self) -> None:
        """Background thread to retry sending buffered batches."""
        while True:
            time.sleep(10)  # Check every 10 seconds
            if not self.disk_buffer or breaker.state == CircuitState.OPEN:
                continue
            
            count = self.disk_buffer.get_count()
            if count > 0:
                logger.info("recovering_buffered_batches", count=count)
                # Process in small batches to not block other flushes
                batches = self.disk_buffer.pop_batch(limit=10)
                for _, topic, frame in batches:
                    try:
                        self._send_frame(frame)
                        # We don't record metrics here to avoid double counting
                        # (they were recorded when originally attempted, but wait, 
                        # they weren't because they failed. Actually, let's record them.)
                        # Actually, raw_len is in the header, but we'd have to unpack it.
                        # For simplicity, we'll just log success.
                    except Exception:
                        # Re-buffer if it fails again
                        self.disk_buffer.push(topic, frame)
                        break # Stop recovery if it fails

    def _health_server(self) -> None:
        """Starts a basic health check HTTP server."""
        HealthHandler.agent = self
        server = HTTPServer(('0.0.0.0', settings.health_check_port), HealthHandler)
        logger.info("health_server_started", port=settings.health_check_port)
        server.serve_forever()

    def _metrics_reporter(self) -> None:
        """Periodic metrics reporter."""
        while True:
            time.sleep(60)  # Report every minute
            stats = self.metrics.get_stats()
            breaker_stats = breaker.stats
            logger.info("edge_agent_metrics", 
                        stats=stats, 
                        circuit_breaker=breaker_stats)

    def _get_sock(self) -> socket.socket:
        """Return the cached TCP socket, creating a new one if needed."""
        if self._sock is None:
            self._sock = socket.create_connection(
                (settings.collector_host, settings.collector_port), 
                timeout=2
            )
        return self._sock

    def _close_sock(self) -> None:
        """Close the cached socket if open."""
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    def close(self) -> None:
        """Clean shutdown: close the persistent socket."""
        self._close_sock()
        logger.info("edge_agent_stopped", stats=self.metrics.get_stats())

    def enqueue(self, topic: str, msg: dict) -> None:
        """Normalize and add a message to the buffer for its topic."""
        data = normalize_message(msg)
        with self.lock:
            buf = self.buffers[topic]
            q: Deque[bytes] = buf["q"]
            if not q:
                buf["t0"] = time.time() * 1000  # record first message time in ms
            q.append(data)
        # attempt immediate flush if threshold reached
        self._flush_if_needed(topic)

    def _flush_if_needed(self, topic: str) -> None:
        msgs = []
        with self.lock:
            buf = self.buffers[topic]
            q: Deque[bytes] = buf["q"]
            if not q:
                return
            t0 = buf["t0"]
            age = (time.time() * 1000 - t0) if t0 else 0
            if len(q) >= settings.batch_max or age >= settings.batch_ms:
                # detach messages for flushing
                msgs = list(q)
                buf["q"] = deque()
                buf["t0"] = None
        
        # release lock while compressing and sending
        if msgs:
            self._flush_batch(topic, msgs)

    def _background_flusher(self) -> None:
        """Periodic flusher; wakes up periodically to flush aged batches."""
        while True:
            time.sleep(settings.batch_ms / 1000.0)
            topics_to_flush = []
            with self.lock:
                for topic, buf in self.buffers.items():
                    q: Deque[bytes] = buf["q"]
                    if not q:
                        continue
                    t0 = buf["t0"]
                    age = (time.time() * 1000 - t0) if t0 else 0
                    if age >= settings.batch_ms:
                        msgs = list(q)
                        buf["q"] = deque()
                        buf["t0"] = None
                        topics_to_flush.append((topic, msgs))
            
            # flush outside lock
            for topic, msgs in topics_to_flush:
                self._flush_batch(topic, msgs)

    @breaker
    def _send_frame(self, frame: bytes) -> None:
        """Send frame to collector with circuit breaker protection."""
        try:
            self._get_sock().sendall(frame)
        except Exception as e:
            self._close_sock()
            raise e

    def _flush_batch(self, topic: str, msgs: List[bytes]) -> None:
        raw = b"\n".join(msgs) + b"\n"
        raw_len = len(raw)
        
        # choose dict for this topic; default to no compression if missing
        if topic not in self.compressors:
            comp_payload = raw
            dict_id = ""
        else:
            comp = self.compressors[topic]["compressor"]
            dict_id = self.compressors[topic]["dict_id"]
            comp_payload = comp.compress(raw)
            
        header = {
            "v": 1,
            "topic": topic,
            "codec": "zstd",
            "level": settings.compression_level,
            "dict_id": dict_id,
            "schema_id": "s1",
            "count": len(msgs),
            "raw_len": raw_len,
            "comp_len": len(comp_payload),
            "t0": msgs and (time.time()),
            "t1": time.time(),
        }
        
        hdr_bytes = json.dumps(header, separators=(",", ":")).encode()
        frame = struct.pack("!I", len(hdr_bytes)) + hdr_bytes + comp_payload
        
        # send to collector using the circuit-breaker protected method
        try:
            self._send_frame(frame)
            ratio = len(comp_payload) / max(raw_len, 1)
            self.metrics.record_batch(len(msgs), raw_len, len(comp_payload))
            logger.info("batch_flushed", 
                        topic=topic, 
                        message_count=len(msgs), 
                        raw_len=raw_len, 
                        comp_len=len(comp_payload), 
                        ratio=f"{ratio:.2%}")
        except Exception as e:
            if self.disk_buffer:
                self.disk_buffer.push(topic, frame)
                logger.warning("batch_buffered", topic=topic, error=e, 
                              buffer_count=self.disk_buffer.get_count())
            else:
                logger.error("batch_flush_failed", topic=topic, error=e)


def synth_feed(agent: EdgeAgent) -> None:
    """Generate synthetic vehicle telemetry messages for testing."""
    import random
    import time
    topics = ["vehicle.telemetry", "vehicle.bounding_boxes", "sensor.engine"]
    i = 0
    while True:
        for topic in topics:
            msg = {
                "topic": topic,
                "device_id": "vehicle-ox-123",
                "status": "OPERATIONAL",
                "data": {
                    "velocity": 20.0 + (i % 5), # Highly repetitive
                    "location": {"lat": 37.7749, "lng": -122.4194},
                    "engine_temp": 90.0 + (i % 2)
                },
                "headers": {
                    "X-Amzn-Trace-Id": f"Root={random.randint(1,10**8)}",
                }
            }
            agent.enqueue(topic, msg)
            i += 1
            time.sleep(0.01) # ~100 msgs/s


if __name__ == "__main__":
    agent = EdgeAgent()
    agent.start()
    # For demonstration, run the synthetic feeder
    synth_feed(agent)
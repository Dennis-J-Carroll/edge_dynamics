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
from typing import Any, Deque, Dict, List

import ujson  # type: ignore
import zstandard as zstd
import psutil
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from edge_dynamics.config import get_settings
from edge_dynamics.structured_logging import get_logger
from edge_dynamics.circuit_breaker import CircuitBreaker, CircuitState
from edge_dynamics.disk_buffer import DiskBuffer
from edge_dynamics.security import sign_frame, create_client_context
from edge_dynamics.edge_utils.dict_lifecycle import DictionaryLifecycleManager
from edge_dynamics.edge_utils.connection_pool import ConnectionPool

# Initialize configuration and logging
settings = get_settings()
logger = get_logger("edge_agent")

# Initialize circuit breaker for collector connection
breaker = CircuitBreaker(
    failure_threshold=5,
    timeout=5.0,
    name="collector_connection"
)

# ---------------------------------------------------------------------------
# Bounded thread pool for the health HTTP server
# ---------------------------------------------------------------------------

class _BoundedThreadHTTPServer(ThreadingHTTPServer):
    """ThreadingHTTPServer capped at max_workers threads.

    Plain ThreadingHTTPServer spawns one thread per request indefinitely.
    With Express polling every 2s this accumulates threads over time.
    A ThreadPoolExecutor bounds concurrency and reuses threads.
    """
    def __init__(self, *args: Any, max_workers: int = 4, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

    def process_request(self, request: Any, client_address: Any) -> None:  # type: ignore[override]
        self._executor.submit(self.process_request_thread, request, client_address)

    def server_close(self) -> None:
        super().server_close()
        self._executor.shutdown(wait=False)


# ---------------------------------------------------------------------------
# Health response cache — 1s TTL shared across all concurrent requests
# ---------------------------------------------------------------------------

_HEALTH_CACHE_TTL = 1.0  # seconds

class _HealthCache:
    """Thread-safe byte cache with TTL.

    All concurrent do_GET calls within the TTL window share a single
    computed response, eliminating redundant lock acquisitions on the
    connection pool, metrics, and DLM.
    """
    __slots__ = ("_lock", "_data", "_expires")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._data: bytes | None = None
        self._expires: float = 0.0

    def get(self) -> bytes | None:
        with self._lock:
            return self._data if time.monotonic() < self._expires else None

    def put(self, data: bytes) -> None:
        with self._lock:
            self._data = data
            self._expires = time.monotonic() + _HEALTH_CACHE_TTL

_health_cache = _HealthCache()

class HealthHandler(BaseHTTPRequestHandler):
    """Simple HTTP handler for health checks."""
    agent: Any = None
    
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            # Serve cached response if still fresh — avoids redundant lock
            # acquisitions on every concurrent poll within the TTL window.
            cached = _health_cache.get()
            if cached is not None:
                self.wfile.write(cached)
                return

            pool_stats = self.agent.connection_pool.get_stats()
            dlm_stats  = self.agent.dlm.get_stats()
            m          = self.agent.metrics.get_stats()

            health = {
                "status": "healthy" if breaker.state.value.lower() != "open" else "degraded",
                "timestamp": time.time(),
                "transport": {
                    "circuit_breaker":     breaker.state.value,
                    "collector_reachable": self._check_collector(),
                    "pool_size":           pool_stats.get("size", 0),
                    "pool_reuse_ratio":    pool_stats.get("reused", 0) / max(pool_stats.get("created", 1), 1),
                },
                "compression": {
                    "ratio":              m["compression_ratio"],
                    "bytes_in":           m["bytes_in"],
                    "bytes_out":          m["bytes_out"],
                    "bytes_saved":        m["bytes_in"] - m["bytes_out"],
                    "messages_processed": m["messages_processed"],
                    "batch_count":        m["batch_count"],
                    "active_dicts":       len(dlm_stats.get("versions", {})),
                },
                "backpressure": {
                    "queue_depth":    sum(len(b["q"]) for b in self.agent.buffers.values()),
                    "memory_percent": psutil.virtual_memory().percent,
                },
                "recovery": {
                    "buffered_frames":  self.agent.disk_buffer.get_count() if self.agent.disk_buffer else 0,
                    "storage_migration": "sqlite",
                },
                "security": {
                    "tls_enabled":  settings.tls_enabled,
                    "hmac_enabled": settings.auth_enabled,
                },
            }
            payload = json.dumps(health).encode()
            _health_cache.put(payload)
            self.wfile.write(payload)
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
        # v2: Adaptive dictionary lifecycle (replaces static dict loading)
        self.dlm = DictionaryLifecycleManager(
            dict_dir=settings.dict_dir,
            compression_level=settings.compression_level,
            dict_size=8192,
            old_dict_ttl_seconds=300.0,
            on_retrain_complete=self._on_dict_retrained,
            monitor_kwargs={
                "warmup_batches": 20,
                "drift_threshold": 0.20,
                "consecutive_trigger": 3,
                "alpha": 0.10,
            },
        )
        self.dlm.load_from_index()
        # Kept for backwards compatibility; DLM is authoritative now
        self.dict_index: Dict[str, dict] = {}
        self.compressors: Dict[str, dict] = {}
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

        ssl_ctx = create_client_context(settings) if settings.tls_enabled else None
        self.connection_pool = ConnectionPool(
            host=settings.collector_host,
            port=settings.collector_port,
            max_size=settings.connection_pool_size,
            timeout=2.0,
            ssl_context=ssl_ctx,
        )
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
        """Background thread to retry sending buffered batches.

        Drains in a continuous inner loop rather than one batch per 10s cycle,
        with a 0.5s pause between batches to avoid bursting the collector and
        re-tripping the circuit breaker.  The inner loop exits as soon as the
        breaker opens or the buffer empties.
        """
        while True:
            time.sleep(5)  # Wait before checking (was 10s)
            if not self.disk_buffer or breaker.state == CircuitState.OPEN:
                continue

            count = self.disk_buffer.get_count()
            if count == 0:
                continue

            logger.info("recovering_buffered_batches", count=count)
            recovered = 0

            # Drain continuously until the buffer empties or the breaker opens.
            while breaker.state != CircuitState.OPEN:
                batches = self.disk_buffer.pop_batch(limit=10)
                if not batches:
                    break  # Buffer empty

                failed = False
                for _, topic, frame in batches:
                    try:
                        self._send_frame(frame)
                        recovered += 1
                    except Exception:
                        self.disk_buffer.push(topic, frame)
                        failed = True
                        break

                if failed:
                    break

                time.sleep(0.5)  # Pace between batches — prevents pipe-burst

            if recovered:
                remaining = self.disk_buffer.get_count()
                logger.info("recovery_progress", sent=recovered, remaining=remaining)

    def _health_server(self) -> None:
        """Starts a basic health check HTTP server."""
        HealthHandler.agent = self
        server = _BoundedThreadHTTPServer(('0.0.0.0', settings.health_check_port), HealthHandler, max_workers=4)
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
                        circuit_breaker=breaker_stats,
                        pool_stats=self.connection_pool.get_stats())

    def _on_dict_retrained(
        self,
        manager: DictionaryLifecycleManager,
        topic: str,
        new_version: Any,
    ) -> None:
        """Callback fired after each successful background retrain."""
        logger.info("dict_retrained",
                    topic=topic,
                    new_dict_id=new_version.dict_id,
                    new_version=new_version.version,
                    dict_size=new_version.size_bytes)

    def close(self) -> None:
        """Clean shutdown: close all pooled connections."""
        self.connection_pool.close_all()
        logger.info("edge_agent_stopped", stats=self.metrics.get_stats())

    def enqueue(self, topic: str, msg: dict) -> None:
        """Normalize and add a message to the buffer for its topic."""
        data = normalize_message(msg)
        # Feed sample buffer for adaptive retraining
        self.dlm.record_sample(topic, data)
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
        """Send frame to collector using the connection pool."""
        with self.connection_pool.get_connection() as sock:
            sock.sendall(frame)

    def _flush_batch(self, topic: str, msgs: List[bytes]) -> None:
        raw = b"\n".join(msgs) + b"\n"
        raw_len = len(raw)

        # Query DLM for active compressor (blue/green safe)
        comp_info = self.dlm.get_compressor(topic)
        if comp_info is None:
            comp_payload = raw
            dict_id = ""
        else:
            try:
                comp_payload = comp_info["compressor"].compress(raw)
                dict_id = comp_info["dict_id"]
            except Exception as e:
                logger.error("compression_failed", topic=topic, error=e)
                comp_payload = raw
                dict_id = ""
            
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

        # HMAC: sign the compressed payload and embed the signature
        if settings.auth_enabled and settings.auth_secret_key:
            header[settings.hmac_header_name] = sign_frame(
                comp_payload,
                settings.auth_secret_key,
                algorithm=settings.hmac_algorithm,
            )
            header["hmac_algo"] = settings.hmac_algorithm

        hdr_bytes = json.dumps(header, separators=(",", ":")).encode()
        frame = struct.pack("!I", len(hdr_bytes)) + hdr_bytes + comp_payload
        
        # send to collector using the circuit-breaker protected method
        try:
            self._send_frame(frame)
            ratio = len(comp_payload) / max(raw_len, 1)
            self.metrics.record_batch(len(msgs), raw_len, len(comp_payload))
            # Update EMA ratio monitor — triggers background retrain on drift
            event = self.dlm.update_ratio(topic, raw_len, len(comp_payload))
            if event.event_type == "drift_detected":
                logger.warning("compression_ratio_drift",
                               topic=topic,
                               ema=round(event.ema, 4),
                               baseline=round(event.baseline, 4) if event.baseline else None,
                               drift_pct=round(event.drift_magnitude / max(event.baseline or 1e-9, 1e-9) * 100, 1))
            elif event.event_type == "warmup_complete":
                logger.info("compression_baseline_established",
                            topic=topic, baseline=round(event.ema, 4))
            elif event.event_type == "recovered":
                logger.info("compression_ratio_recovered",
                            topic=topic, ema=round(event.ema, 4))
            logger.info("batch_flushed",
                        topic=topic,
                        message_count=len(msgs),
                        raw_len=raw_len,
                        comp_len=len(comp_payload),
                        ratio=f"{ratio:.2%}",
                        dict_id=dict_id)
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


def main() -> None:
    """Entry point for the edge-agent console script."""
    agent = EdgeAgent()
    agent.start()
    # For demonstration, run the synthetic feeder
    synth_feed(agent)


if __name__ == "__main__":
    main()
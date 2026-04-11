#!/usr/bin/env python3
"""
collector_server.py

Simple TCP collector for compressed batches produced by an edge agent.
Each frame consists of a 4‑byte big‑endian length prefix, a JSON header
describing the payload, followed by the compressed payload bytes.
The server loads per‑topic dictionaries from dict_index.json and uses
zstandard to decompress incoming payloads.
It writes decompressed JSON lines to per‑topic files under the output
directory and appends metrics to a CSV file.

Usage:
    python3 collector_server.py
"""

import csv
import json
import os
import signal
import socket
import struct
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, IO, Tuple

import zstandard as zstd

from edge_dynamics.config import get_settings
from edge_dynamics.edge_utils.logging import get_logger
from edge_dynamics.security import verify_frame, create_server_context

logger = get_logger("collector")

# Load configuration (paths are CWD-relative by default, not __file__-relative)
_settings = get_settings()
HOST, PORT = "0.0.0.0", _settings.collector_port
DICT_DIR = _settings.dict_dir
OUT_DIR = _settings.out_dir
METRICS = _settings.metrics_file


def load_dictionaries() -> Tuple[Dict[str, dict], Dict[str, zstd.ZstdDecompressor]]:
    """Load dictionaries and return mapping of dict_id to decompressors."""
    index_path = os.path.join(DICT_DIR, "dict_index.json")
    if not os.path.exists(index_path):
        logger.warning("dict_index_not_found", path=index_path)
        return {}, {}
    with open(index_path, "r") as f:
        index = json.load(f)
    dec: Dict[str, zstd.ZstdDecompressor] = {}
    for topic, meta in index.items():
        path = meta["path"]
        with open(path, "rb") as fd:
            dict_data = fd.read()
            zd = zstd.ZstdCompressionDict(dict_data)
        dec[meta["dict_id"]] = zstd.ZstdDecompressor(dict_data=zd)
    logger.info("dictionaries_loaded", count=len(dec))
    return index, dec


def recvall(sock: socket.socket, n: int) -> bytes:
    """Receive exactly n bytes from socket."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Socket closed before receiving expected bytes")
        buf.extend(chunk)
    return bytes(buf)


def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    # Ensure metrics CSV exists with header
    if not os.path.exists(METRICS):
        with open(METRICS, "w", newline="") as f:
            csv.writer(f).writerow([
                "timestamp",
                "topic",
                "count",
                "raw_bytes",
                "compressed_bytes",
                "ratio",
                "dict_id",
            ])

    dict_index, decompressors = load_dictionaries()

    # SIGHUP: hot-reload dict_index.json after DLM retrains a dictionary
    def reload_dicts(signum: int, frame: object) -> None:
        nonlocal dict_index, decompressors
        logger.info("sighup_reloading_dicts")
        try:
            dict_index, decompressors = load_dictionaries()
            logger.info("dicts_reloaded", count=len(decompressors))
        except Exception as exc:
            logger.error("dict_reload_failed", error=str(exc))

    signal.signal(signal.SIGHUP, reload_dicts)

    # Cached file handles: per-topic JSONL files and shared metrics CSV
    jsonl_handles: Dict[str, IO[bytes]] = {}
    jsonl_lock = threading.Lock()
    metrics_handle = open(METRICS, "a", newline="")
    metrics_writer = csv.writer(metrics_handle)
    metrics_lock = threading.Lock()

    def get_jsonl_handle(topic: str) -> IO[bytes]:
        """Return a cached file handle for the topic's JSONL output."""
        if topic not in jsonl_handles:
            with jsonl_lock:
                if topic not in jsonl_handles:
                    out_path = os.path.join(OUT_DIR, f"{topic}.jsonl")
                    jsonl_handles[topic] = open(out_path, "ab")
        return jsonl_handles[topic]

    def handle_connection(conn: socket.socket) -> None:
        """Process incoming frames from an edge agent over a persistent connection."""
        with conn:
            while True:
                try:
                    header_len_bytes = recvall(conn, 4)
                    header_len = struct.unpack("!I", header_len_bytes)[0]
                    header_data = recvall(conn, header_len)
                    header = json.loads(header_data)
                    compressed_len = header["comp_len"]
                    comp_payload = recvall(conn, compressed_len)

                    # HMAC: verify FIRST — before decompressing any untrusted bytes.
                    # Decompressing attacker-controlled data before authentication
                    # opens the door to zip-bomb and parser-confusion attacks.
                    if _settings.auth_enabled and _settings.auth_secret_key:
                        sig_key = _settings.hmac_header_name
                        signature = header.get(sig_key)
                        algo = header.get("hmac_algo", _settings.hmac_algorithm)
                        if not signature or not verify_frame(
                            comp_payload, signature, _settings.auth_secret_key, algorithm=algo
                        ):
                            logger.warning(
                                "frame_rejected_hmac",
                                topic=header.get("topic", "?"),
                                reason="missing_or_invalid_signature",
                            )
                            continue

                    # Now safe to decompress
                    dict_id = header["dict_id"]
                    if dict_id:
                        if dict_id not in decompressors:
                            raise ValueError(f"Unknown dict_id {dict_id!r}")
                        decompressed = decompressors[dict_id].decompress(comp_payload)
                    else:
                        decompressed = comp_payload  # uncompressed fallback

                    # Write decompressed data to cached per-topic file handle
                    topic = header["topic"]
                    fh = get_jsonl_handle(topic)
                    fh.write(decompressed)
                    fh.flush()

                    # Append metrics under lock to prevent interleaved writes
                    ratio = header["comp_len"] / max(header["raw_len"], 1)
                    with metrics_lock:
                        metrics_writer.writerow([
                            time.time(),
                            topic,
                            header["count"],
                            header["raw_len"],
                            header["comp_len"],
                            ratio,
                            dict_id,
                        ])
                        metrics_handle.flush()

                    logger.info(
                        "frame_received",
                        topic=topic,
                        count=header["count"],
                        raw_len=header["raw_len"],
                        comp_len=header["comp_len"],
                        ratio=f"{ratio:.2%}",
                        dict_id=dict_id or "none",
                    )

                except (ConnectionError, EOFError):
                    break  # connection closed normally
                except Exception as exc:
                    logger.error("frame_error", error=str(exc))
                    break

    # TLS: wrap the server socket if enabled
    ssl_ctx = None
    if _settings.tls_enabled:
        ssl_ctx = create_server_context(_settings)
        logger.info(
            "tls_enabled",
            verify_client=_settings.tls_verify_client,
        )

    with socket.create_server((HOST, PORT), reuse_port=True) as srv:
        logger.info("collector_listening", host=HOST, port=PORT)
        with ThreadPoolExecutor(max_workers=_settings.connection_pool_size) as pool:
            while True:
                conn, addr = srv.accept()
                if ssl_ctx:
                    try:
                        conn = ssl_ctx.wrap_socket(conn, server_side=True)
                    except Exception as exc:
                        logger.warning(
                            "tls_handshake_failed",
                            addr=str(addr),
                            error=str(exc),
                        )
                        conn.close()
                        continue
                pool.submit(handle_connection, conn)


if __name__ == "__main__":
    main()

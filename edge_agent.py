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
from typing import Deque, Dict, Optional

import ujson  # type: ignore
import zstandard as zstd


# Configuration
COLLECTOR = ("127.0.0.1", 7000)  # Address of the collector
DICT_DIR = os.path.join(os.path.dirname(__file__), "dicts")
BATCH_MAX = 100  # Maximum messages per batch before flush
BATCH_MS = 250   # Maximum age in milliseconds before flush
COMPRESSION_LEVEL = 7  # zstd compression level


def load_dictionaries() -> Dict[str, dict]:
    """Load dictionary index and return a mapping of topic -> dict info."""
    index_path = os.path.join(DICT_DIR, "dict_index.json")
    with open(index_path, "r") as f:
        return json.load(f)


def build_compressors(index: Dict[str, dict]) -> Dict[str, dict]:
    """Build zstd compressors for each topic based on loaded dictionaries."""
    compressors: Dict[str, dict] = {}
    for topic, meta in index.items():
        with open(meta["path"], "rb") as f:
            zdict = zstd.ZstdDictionary(f.read())
        compressors[topic] = {
            "dict_id": meta["dict_id"],
            "compressor": zstd.ZstdCompressor(level=COMPRESSION_LEVEL, dict_data=zdict),
        }
    return compressors


def normalize_message(msg: dict) -> bytes:
    """Remove volatile headers and serialize message to compact JSON bytes."""
    headers = msg.get("headers")
    if isinstance(headers, dict):
        headers.pop("X-Amzn-Trace-Id", None)
    # Use ujson for faster serialization; ensure compact form with separators
    return ujson.dumps(msg, ensure_ascii=False, separators=(",", ":")).encode()


class EdgeAgent:
    def __init__(self) -> None:
        self.dict_index = load_dictionaries()
        self.compressors = build_compressors(self.dict_index)
        self.buffers: Dict[str, Dict[str, Optional[float] or Deque[bytes]]] = defaultdict(
            lambda: {"q": deque(), "t0": None}
        )
        self.lock = threading.Lock()
        self.flusher_thread = threading.Thread(target=self._background_flusher, daemon=True)

    def start(self) -> None:
        self.flusher_thread.start()

    def enqueue(self, topic: str, msg: dict) -> None:
        """Normalize and add a message to the buffer for its topic."""
        data = normalize_message(msg)
        with self.lock:
            buf = self.buffers[topic]
            q: Deque[bytes] = buf["q"]  # type: ignore[assignment]
            if not q:
                buf["t0"] = time.time() * 1000  # record first message time in ms
            q.append(data)
        # attempt immediate flush if threshold reached
        self._flush_if_needed(topic)

    def _flush_if_needed(self, topic: str) -> None:
        with self.lock:
            buf = self.buffers[topic]
            q: Deque[bytes] = buf["q"]  # type: ignore[assignment]
            if not q:
                return
            t0 = buf["t0"]
            age = (time.time() * 1000 - t0) if t0 else 0
            if len(q) >= BATCH_MAX or age >= BATCH_MS:
                # detach messages for flushing
                msgs = list(q)
                buf["q"] = deque()
                buf["t0"] = None
            else:
                return
        # release lock while compressing and sending
        if msgs:
            self._flush_batch(topic, msgs)

    def _background_flusher(self) -> None:
        """Periodic flusher; wakes up periodically to flush aged batches."""
        while True:
            time.sleep(BATCH_MS / 1000.0)
            topics = []
            with self.lock:
                for topic, buf in self.buffers.items():
                    q: Deque[bytes] = buf["q"]  # type: ignore[assignment]
                    if not q:
                        continue
                    t0 = buf["t0"]
                    age = (time.time() * 1000 - t0) if t0 else 0
                    if age >= BATCH_MS:
                        msgs = list(q)
                        buf["q"] = deque()
                        buf["t0"] = None
                        topics.append((topic, msgs))
            # flush outside lock
            for topic, msgs in topics:
                self._flush_batch(topic, msgs)

    def _flush_batch(self, topic: str, msgs: list) -> None:
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
            "level": COMPRESSION_LEVEL,
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
        # send to collector
        try:
            s = socket.create_connection(COLLECTOR, timeout=2)
            s.sendall(frame)
            s.close()
            ratio = len(comp_payload) / max(raw_len, 1)
            print(
                f"[flush] {topic}: {len(msgs)} msgs raw={raw_len} comp={len(comp_payload)} ({ratio:.2%})"
            )
        except Exception as e:
            print(f"[edge_agent] send error: {e}")


def synth_feed(agent: EdgeAgent) -> None:
    """Generate synthetic file telemetry messages for testing."""
    import random
    import string
    topics = ["files.txt", "files.csv", "files.json"]
    while True:
        for topic in topics:
            # create a fake message with file metadata
            filename = ''.join(random.choices(string.ascii_lowercase, k=8)) + "." + topic.split(".")[-1]
            msg = {
                "file_type": topic.split(".")[-1],
                "path": f"/var/log/{topic}/{filename}",
                "size": random.randint(10, 10_000),
                "checksum": ''.join(random.choices('abcdef0123456789', k=8)),
                "headers": {
                    "Accept": "*/*",
                    "User-Agent": "edge-agent/1.0",
                    "X-Amzn-Trace-Id": f"Root={random.randint(1,10**8)}-{random.randint(10**16,10**17-1)}",
                },
            }
            agent.enqueue(topic, msg)
            time.sleep(0.005)  # 200 msgs/s aggregate


if __name__ == "__main__":
    agent = EdgeAgent()
    agent.start()
    # For demonstration, run the synthetic feeder
    synth_feed(agent)
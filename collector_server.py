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
import socket
import struct
import time
from typing import Dict, Tuple

import zstandard as zstd


HOST, PORT = "0.0.0.0", 7000
# Directory containing dictionaries and index
DICT_DIR = os.path.join(os.path.dirname(__file__), "dicts")
# Directory to write decompressed JSONL files
OUT_DIR = os.path.join(os.path.dirname(__file__), "out")
# CSV file to append metrics
METRICS = os.path.join(os.path.dirname(__file__), "metrics.csv")


def load_dictionaries() -> Tuple[Dict[str, dict], Dict[str, zstd.ZstdDecompressor]]:
    """Load dictionaries and return mapping of dict_id to decompressors."""
    index_path = os.path.join(DICT_DIR, "dict_index.json")
    with open(index_path, "r") as f:
        index = json.load(f)
    dec: Dict[str, zstd.ZstdDecompressor] = {}
    for topic, meta in index.items():
        path = meta["path"]
        with open(path, "rb") as fd:
            zd = zstd.ZstdDictionary(fd.read())
        dec[meta["dict_id"]] = zstd.ZstdDecompressor(dict_data=zd)
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

    with socket.create_server((HOST, PORT), reuse_port=True) as srv:
        print(f"[collector] listening on {HOST}:{PORT}")
        while True:
            conn, addr = srv.accept()
            with conn:
                try:
                    header_len_bytes = recvall(conn, 4)
                    header_len = struct.unpack("!I", header_len_bytes)[0]
                    header_data = recvall(conn, header_len)
                    header = json.loads(header_data)
                    compressed_len = header["comp_len"]
                    comp_payload = recvall(conn, compressed_len)
                    dict_id = header["dict_id"]
                    if dict_id not in decompressors:
                        raise ValueError(f"Unknown dict_id {dict_id}")
                    decompressor = decompressors[dict_id]
                    decompressed = decompressor.decompress(comp_payload)
                    # Write decompressed data to per‑topic file
                    topic = header["topic"]
                    out_path = os.path.join(OUT_DIR, f"{topic}.jsonl")
                    with open(out_path, "ab") as out_file:
                        out_file.write(decompressed)
                    # Append metrics
                    with open(METRICS, "a", newline="") as f:
                        csv.writer(f).writerow([
                            time.time(),
                            topic,
                            header["count"],
                            header["raw_len"],
                            header["comp_len"],
                            header["comp_len"] / max(header["raw_len"], 1),
                            dict_id,
                        ])
                    print(
                        f"[collector] received {header['count']} msgs for {topic}: raw={header['raw_len']} bytes, comp={header['comp_len']} bytes ({header['comp_len']/max(header['raw_len'],1):.2%})"
                    )
                except Exception as exc:
                    print(f"[collector] error: {exc}")


if __name__ == "__main__":
    main()
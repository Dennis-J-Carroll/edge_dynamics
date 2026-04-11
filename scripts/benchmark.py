#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
edge_dynamics benchmark suite

Three benchmarks, each testing a different layer:

  compression  — raw JSON vs raw-zstd vs dict-zstd (pure in-process)
  throughput   — msg/s through the full compress+frame pipeline (mock socket)
  recovery     — ms from collector outage to first successful re-send (real TCP)

Usage:
    cd edge_dynamics
    python scripts/benchmark.py                   # all benchmarks, table output
    python scripts/benchmark.py --json            # JSON only (CI / README)
    python scripts/benchmark.py --skip-recovery   # skip TCP-dependent test
"""

import argparse
import json
import logging
import random
import socket
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import ujson
import zstandard as zstd

_REPO = Path(__file__).parent.parent
sys.path.insert(0, str(_REPO / "src"))

from edge_dynamics.circuit_breaker import CircuitBreaker, CircuitState
from edge_dynamics.config import Settings
from edge_dynamics.edge_agent import EdgeAgent


# ── synthetic message factory ────────────────────────────────────────────────

def _make_msg(i: int = 0) -> dict:
    """Synthetic vehicle telemetry — identical schema to the synth_feed."""
    return {
        "topic": "vehicle.telemetry",
        "device_id": "vehicle-ox-123",
        "status": "OPERATIONAL",
        "data": {
            "velocity": 20.0 + (i % 5),
            "location": {"lat": 37.7749, "lng": -122.4194},
            "engine_temp": 90.0 + (i % 2),
        },
        "headers": {"X-Amzn-Trace-Id": f"Root={random.randint(1, 10 ** 8)}"},
    }


def _normalize(msg: dict) -> bytes:
    """Same normalization the agent applies: ujson + sorted keys."""
    return ujson.dumps(msg, sort_keys=True).encode()


# ── helpers for creating isolated agents ────────────────────────────────────

def _test_settings(port: int, batch_max: int, batch_ms: int = 500, **extra) -> Settings:
    return Settings(
        collector_host="127.0.0.1",
        collector_port=port,
        disk_buffer_enabled=False,
        batch_max=batch_max,
        batch_ms=batch_ms,
        dict_dir=str(_REPO / "dicts"),
        auth_enabled=False,
        tls_enabled=False,
        **extra,
    )


def _mock_pool() -> MagicMock:
    """Return a ConnectionPool mock that records sendall calls."""
    sent = []
    sock = MagicMock()
    sock.sendall.side_effect = lambda data: sent.append(len(data))
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=sock)
    ctx.__exit__ = MagicMock(return_value=False)
    pool = MagicMock()
    pool.get_connection.return_value = ctx
    pool.get_stats.return_value = {}
    pool.close_all = MagicMock()
    pool._sent = sent
    return pool


# ── benchmark 1: compression ─────────────────────────────────────────────────

def bench_compression(n_msgs: int = 100) -> dict:
    """
    Compare three compression strategies on n_msgs synthetic messages.

    Trains a zstd dictionary inline (8 KB) so the result is self-contained.
    The training set is distinct from the measurement set to avoid overfitting.
    """
    # --- build payloads ---
    train_payloads = [_normalize(_make_msg(i)) for i in range(n_msgs * 2)]
    test_payloads  = [_normalize(_make_msg(i)) for i in range(n_msgs * 2, n_msgs * 3)]
    test_batch     = b"\n".join(test_payloads)

    raw_bytes = sum(len(p) for p in test_payloads)

    # --- raw zstd, no dict ---
    cctx = zstd.ZstdCompressor(level=7)
    comp_no_dict = cctx.compress(test_batch)

    # --- zstd + inline-trained dict ---
    try:
        zd = zstd.train_dictionary(8192, train_payloads)
        cctx_dict = zstd.ZstdCompressor(level=7, dict_data=zd)
        comp_dict = cctx_dict.compress(test_batch)
        dict_bytes = len(comp_dict)
        dict_savings = round((1 - dict_bytes / raw_bytes) * 100, 1)
    except Exception as exc:
        dict_bytes = None
        dict_savings = None

    methods = [
        {
            "method": "raw JSON",
            "bytes_per_msg": round(raw_bytes / n_msgs),
            "ratio": 1.0,
            "savings_pct": 0.0,
        },
        {
            "method": "zstd (no dict)",
            "bytes_per_msg": round(len(comp_no_dict) / n_msgs),
            "ratio": round(len(comp_no_dict) / raw_bytes, 4),
            "savings_pct": round((1 - len(comp_no_dict) / raw_bytes) * 100, 1),
        },
    ]
    if dict_bytes is not None:
        methods.append({
            "method": "zstd + dict (8 KB)",
            "bytes_per_msg": round(dict_bytes / n_msgs),
            "ratio": round(dict_bytes / raw_bytes, 4),
            "savings_pct": dict_savings,
        })

    return {"n_msgs": n_msgs, "methods": methods}


# ── benchmark 2: throughput ───────────────────────────────────────────────────

def bench_throughput(reps: int = 20) -> dict:
    """
    Measure end-to-end compress+frame throughput (messages/sec) using a mock
    socket so results reflect CPU cost only, without network variance.

    Calls _flush_batch directly — same code path as production, minus sendall
    latency.  Each (batch_max, rep) pair is timed independently; the median
    is reported to suppress outliers from GC / thread scheduling.
    """
    import statistics

    results = []

    for batch_max in (10, 50, 100, 200):
        settings = _test_settings(port=19999, batch_max=batch_max)
        pool = _mock_pool()

        with patch("edge_dynamics.edge_agent.settings", settings), \
             patch("edge_dynamics.edge_agent.ConnectionPool", return_value=pool):

            fresh_breaker = CircuitBreaker(
                failure_threshold=9999, timeout=1.0, name="bench"
            )
            with patch("edge_dynamics.edge_agent.breaker", fresh_breaker):
                agent = EdgeAgent(
                    enable_flush=False,
                    enable_metrics=False,
                    enable_health=False,
                )

                msgs = [_normalize(_make_msg(i)) for i in range(batch_max)]
                raw_bytes_per_batch = sum(len(m) for m in msgs)

                # Warm up the compressor (first call trains/loads zstd state)
                agent._flush_batch("vehicle.telemetry", msgs)

                times = []
                for _ in range(reps):
                    t0 = time.perf_counter()
                    agent._flush_batch("vehicle.telemetry", msgs)
                    times.append(time.perf_counter() - t0)

        median_s = statistics.median(times)
        results.append({
            "batch_max":         batch_max,
            "reps":              reps,
            "median_batch_ms":   round(median_s * 1000, 3),
            "msg_per_sec":       round(batch_max / median_s),
            "raw_mb_per_sec":    round(raw_bytes_per_batch / median_s / 1e6, 2),
        })

    return {"reps_per_size": reps, "results": results}


# ── benchmark 3: recovery time ────────────────────────────────────────────────

def _start_sink(port: int) -> threading.Event:
    """TCP sink that accepts connections and discards all data.

    Returns a stop event; set it to shut the sink down cleanly.
    """
    stop = threading.Event()

    def _drain(conn: socket.socket) -> None:
        try:
            while not stop.is_set():
                data = conn.recv(65536)
                if not data:
                    break
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _serve() -> None:
        with socket.create_server(("127.0.0.1", port)) as srv:
            srv.settimeout(0.2)
            while not stop.is_set():
                try:
                    conn, _ = srv.accept()
                    threading.Thread(
                        target=_drain, args=(conn,), daemon=True
                    ).start()
                except socket.timeout:
                    continue

    threading.Thread(target=_serve, daemon=True, name="bench-sink").start()
    time.sleep(0.05)  # let the OS bind the socket
    return stop


def bench_recovery(port: int = 17778, timeout_s: float = 15.0) -> dict:
    """
    Measure two latencies:
      outage_detection_ms  — time from sink shutdown to circuit breaker OPEN
      recovery_ms          — time from sink restart to first successful re-send

    Tests the circuit breaker + connection pool stack directly (no agent
    wrapper) so that send failures propagate without being silently buffered.
    """
    from edge_dynamics.edge_utils.connection_pool import ConnectionPool

    CB_FAILURE_THRESHOLD = 3
    CB_TIMEOUT_S = 2.0
    cb = CircuitBreaker(
        failure_threshold=CB_FAILURE_THRESHOLD,
        timeout=CB_TIMEOUT_S,
        name="bench_recovery",
    )
    pool = ConnectionPool(host="127.0.0.1", port=port, max_size=2, timeout=1.0)

    @cb
    def _send(data: bytes) -> None:
        with pool.get_connection() as sock:
            sock.sendall(data)

    probe = b"x" * 256  # small payload, protocol-agnostic

    # ── phase 1: establish initial connectivity ───────────────────────────────
    stop_sink = _start_sink(port)
    deadline = time.time() + timeout_s
    connected = False
    while time.time() < deadline:
        try:
            _send(probe)
            connected = True
            break
        except Exception:
            time.sleep(0.05)

    if not connected:
        stop_sink.set()
        pool.close_all()
        return {"error": "Could not establish initial connection within timeout"}

    # ── phase 2: kill the sink, measure detection latency ────────────────────
    stop_sink.set()
    time.sleep(0.05)  # let OS tear down the socket

    outage_start = time.perf_counter()
    deadline = time.time() + timeout_s
    while cb.state != CircuitState.OPEN:
        try:
            _send(probe)
        except Exception:
            pass
        if time.time() > deadline:
            pool.close_all()
            return {"error": "Circuit breaker did not open within timeout"}
        time.sleep(0.02)

    detection_ms = round((time.perf_counter() - outage_start) * 1000)
    trip_ts = time.perf_counter()

    # ── phase 3: restart sink, poll until first successful re-send ───────────
    stop_sink = _start_sink(port)
    deadline = time.time() + timeout_s
    recovered = False
    while time.time() < deadline:
        try:
            _send(probe)
            # If _send didn't raise, the circuit breaker allowed the call
            # and the send succeeded — we've recovered.
            recovered = True
            break
        except Exception:
            pass
        time.sleep(0.1)

    recovery_ms = round((time.perf_counter() - trip_ts) * 1000)
    stop_sink.set()
    pool.close_all()

    if not recovered:
        return {"error": f"Did not recover within {timeout_s}s"}

    return {
        "outage_detection_ms": detection_ms,
        "recovery_ms":         recovery_ms,
        "circuit_breaker_timeout_s": CB_TIMEOUT_S,
    }


# ── output formatting ─────────────────────────────────────────────────────────

def _col(s: Any, w: int) -> str:
    return str(s).ljust(w)


def print_results(comp: dict, tput: dict, rec: dict | None) -> None:
    sep = "─" * 62

    print(f"\n{'edge_dynamics benchmark':^62}")
    print(sep)

    # compression
    print("\n[1] Compression  ─  100 msgs, vehicle telemetry")
    print(f"  {'Method':<22} {'Bytes/msg':>9} {'Ratio':>7} {'Savings':>8}")
    print(f"  {'─'*22} {'─'*9} {'─'*7} {'─'*8}")
    for m in comp["methods"]:
        print(
            f"  {_col(m['method'], 22)}"
            f" {m['bytes_per_msg']:>9,}"
            f" {m['ratio']:>7.4f}"
            f" {m['savings_pct']:>7.1f}%"
        )

    # throughput
    print(f"\n[2] Throughput  ─  {tput['reps_per_size']} reps per batch size (mock socket)")
    print(f"  {'batch_max':>9} {'msg/s':>9} {'raw MB/s':>9} {'batch ms':>9}")
    print(f"  {'─'*9} {'─'*9} {'─'*9} {'─'*9}")
    for r in tput["results"]:
        print(
            f"  {r['batch_max']:>9,}"
            f" {r['msg_per_sec']:>9,}"
            f" {r['raw_mb_per_sec']:>9.2f}"
            f" {r['median_batch_ms']:>9.3f}"
        )

    # recovery
    print(f"\n[3] Recovery  ─  real TCP circuit-breaker probe")
    if rec is None:
        print("  (skipped)")
    elif "error" in rec:
        print(f"  ERROR: {rec['error']}")
    else:
        print(f"  Outage detection : {rec['outage_detection_ms']:>6} ms")
        print(f"  First re-send    : {rec['recovery_ms']:>6} ms  "
              f"(cb timeout = {rec['circuit_breaker_timeout_s']}s)")

    print(f"\n{sep}\n")


# ── entry point ───────────────────────────────────────────────────────────────

@contextmanager
def _quiet():
    """Suppress structured-log output during benchmarks.

    The edge_dynamics loggers use named handlers with propagate=False and write
    to sys.stdout.  Setting root logger level has no effect on them.  Instead,
    temporarily swap every StreamHandler's stream to /dev/null for the duration
    of the benchmark, then restore the originals.
    """
    import os

    devnull = open(os.devnull, "w")
    swapped: list[tuple[logging.StreamHandler, Any]] = []

    def _silence_all() -> None:
        for obj in logging.Logger.manager.loggerDict.values():
            if not isinstance(obj, logging.Logger):
                continue
            for h in obj.handlers:
                if isinstance(h, logging.StreamHandler):
                    swapped.append((h, h.stream))
                    h.stream = devnull

    _silence_all()
    try:
        yield
    finally:
        for handler, orig in swapped:
            handler.stream = orig
        devnull.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="edge_dynamics benchmark suite")
    parser.add_argument("--json",          action="store_true", help="JSON output only")
    parser.add_argument("--skip-recovery", action="store_true", help="Skip TCP recovery test")
    args = parser.parse_args()

    if not args.json:
        print("Running compression benchmark …", end=" ", flush=True)
    with _quiet():
        comp = bench_compression()
    if not args.json:
        print("done")

    if not args.json:
        print("Running throughput benchmark  …", end=" ", flush=True)
    with _quiet():
        tput = bench_throughput()
    if not args.json:
        print("done")

    rec = None
    if not args.skip_recovery:
        if not args.json:
            print("Running recovery benchmark    …", end=" ", flush=True)
        with _quiet():
            rec = bench_recovery()
        if not args.json:
            print("done")

    if args.json:
        print(json.dumps({"compression": comp, "throughput": tput, "recovery": rec}, indent=2))
    else:
        print_results(comp, tput, rec)


if __name__ == "__main__":
    main()

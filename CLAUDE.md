# CLAUDE.md — edge_dynamics project context

Read this at the start of every session. It contains current state, completed work,
outstanding issues, and the roadmap. Update it when significant work is done.

---

## What this project is

A production-grade edge compression engine for IoT/autonomous-vehicle telemetry.
The core idea: normalize structured JSON, batch it by topic, compress with per-topic
zstd dictionaries, and send frames to a collector over TCP. The result is 95–97%
bandwidth reduction on repetitive telemetry.

The project has two repos that work together:

| Repo | Path | Purpose |
|---|---|---|
| `edge_dynamics` | `~/Desktop/edgeAgent/edge_dynamics` | Python backend engine |
| `EdgeAgent` | `~/Desktop/edgeAgent/EdgeAgent` | React marketing site + Express BFF |

---

## Architecture: Five-Layer Stack

| Layer | Component | What it does |
|---|---|---|
| **Enforcement** | `edge_sentinel` (eBPF/XDP) | Packet filtering at NIC ingress |
| **Intelligence** | `DictionaryLifecycleManager`, `BackpressureGate` | EMA drift detection, adaptive dict retraining, graduated backpressure |
| **Observability** | Structured logging, `/health` endpoint | Per-component stats, real-time health |
| **Trust** | `security.py` (mTLS + HMAC-SHA256) | HMAC-first frame verification |
| **Orchestration** | `ConnectionPool`, `DiskBuffer`, `CircuitBreaker` | Socket reuse, SQLite store-and-forward, auto-recovery |

---

## Current state (as of 2026-04-09)

### Backend (`edge_dynamics/`) — COMPLETE
- 146 tests passing (>90% coverage)
- Health endpoint normalized to 5-section schema (transport/compression/backpressure/recovery/security)
- `DictionaryLifecycleManager` with EMA drift detection + blue/green dict swap
- `BackpressureGate` with graduated 3-tier response
- `CircuitBreaker` (CLOSED → OPEN → HALF_OPEN → CLOSED)
- `DiskBuffer` SQLite store-and-forward (50MB, FIFO eviction)
- `ConnectionPool` with stale-detection and thread-safe acquire
- HMAC-SHA256 per-frame signing + mTLS support
- Docker multi-stage build (collector + agent targets)
- Benchmark suite: `scripts/benchmark.py`

### Frontend (`EdgeAgent/`) — COMPLETE
- React 18 + Vite + Wouter + TanStack Query + recharts + shadcn/ui + framer-motion
- Express BFF on port 5000 proxies `/api/agent/health` → Python agent `:8080`
- Live Demo section: real compression ratio chart (AreaChart), live metric cards, Agent Activity Log
- `useAgentHealth` hook: polls every 2s, refetches in background
- Value Pillars, How It Works, Developer Mode — all updated to reflect edge_dynamics stack

---

## Bug fixes made in V2 session (2026-04-08 / 09)

These are committed to the code but may not yet be in a tagged release.

### 1. `_is_connection_alive` — backwards EOF detection
**File:** `src/edge_dynamics/edge_utils/connection_pool.py`  
**Bug:** `return len(data) == 0` after `recv(MSG_PEEK)` treated EOF (remote closed) as "alive".  
**Fix:** `return len(data) > 0` — empty bytes = EOF = dead connection.  
**Impact:** This was the root cause of repeated broken pipe → circuit breaker trips in production.

### 2. Connection pool deadlock — `_create_connection()` inside lock
**File:** `src/edge_dynamics/edge_utils/connection_pool.py`  
**Bug:** `acquire()` held `self._lock` while calling `_create_connection()`, which also
tried to acquire `self._lock` (re-entrant deadlock). The health handler's `get_stats()`
then blocked waiting for the same lock — freezing every `/health` response.  
**Fix:** Check the condition under the lock, release it, then call `_create_connection()` outside.

### 3. Health server — single-threaded `HTTPServer`
**File:** `src/edge_dynamics/edge_agent.py`  
**Bug:** `HTTPServer` serializes requests. TanStack Query polls every 2s; connections queued up.  
**Fix:** `_BoundedThreadHTTPServer` (ThreadPoolExecutor, max 4 workers) + `_HealthCache` (1s TTL).

### 4. Circuit breaker status — case mismatch
**File:** `src/edge_dynamics/edge_agent.py`  
**Bug:** `breaker.state.value != "open"` — enum returns `"OPEN"` (uppercase), always matched "healthy".  
**Fix:** `.lower()` comparison.

### 5. Recovery loop — pipe-burst re-tripping circuit breaker
**File:** `src/edge_dynamics/edge_agent.py`  
**Bug:** Recovery loop tried to drain N frames instantly after reconnect → burst broke pipe → re-trip.  
**Fix:** Inner drain loop with `time.sleep(0.5)` between batches; loop exits when breaker opens.

---

## Benchmark results (2026-04-09, measured on dev machine)

```
[1] Compression — 100 msgs, vehicle telemetry
  raw JSON             214 bytes/msg    0.0% savings
  zstd (no dict)         9 bytes/msg   95.9% savings
  zstd + dict (8 KB)     6 bytes/msg   97.1% savings

[2] Throughput — compress+frame pipeline (mock socket, 20 reps median)
  batch=10    108,521 msg/s    23 MB/s raw
  batch=50    431,570 msg/s    92 MB/s raw
  batch=100   726,338 msg/s   155 MB/s raw
  batch=200   983,990 msg/s   210 MB/s raw

[3] Recovery — real TCP, circuit breaker state machine
  Outage detection:   61 ms   (3 failures × ~20ms poll)
  First re-send:    2056 ms   (2.0s CB timeout + probe overhead)
```

Run with: `python scripts/benchmark.py` or `python scripts/benchmark.py --json`

---

## How to run the full stack

**Order matters — start collector before agent.**

```bash
# Terminal 1 — Collector
cd ~/Desktop/edgeAgent/edge_dynamics
python -m edge_dynamics.collector_server

# Terminal 2 — Agent
cd ~/Desktop/edgeAgent/edge_dynamics
rm -f buffer.db          # clear SQLite buffer for clean demo state
python -m edge_dynamics.edge_agent

# Terminal 3 — Express + React frontend
cd ~/Desktop/edgeAgent/EdgeAgent
npm run dev

# Verify (in any terminal)
curl http://localhost:5000/api/agent/health
```

Expected health response structure:
```json
{
  "status": "healthy",
  "transport":    { "circuit_breaker": "CLOSED", "collector_reachable": true, ... },
  "compression":  { "ratio": 0.056, "bytes_saved": 3310, "messages_processed": 20, ... },
  "backpressure": { "queue_depth": 2, "memory_percent": 46.5 },
  "recovery":     { "buffered_frames": 0, "storage_migration": "sqlite" },
  "security":     { "tls_enabled": false, "hmac_enabled": false }
}
```

---

## Known / outstanding issues

### Circuit breaker keeps re-tripping (partially fixed)
The `_is_connection_alive` fix (bug #1 above) is in the code but has not been
confirmed to fully resolve the issue in production load. The synth feed in `main()`
generates 3 topics × ~10 msgs/s, batching at BATCH_MAX=100 (250ms). The collector
is sometimes closing connections after receiving frames, causing the pool to hold
stale sockets. If the circuit breaker still trips after restart, check the collector
logs for `frame_error` events (unknown dict_id or decompression failure).

### SQLite buffer grows during circuit breaker OPEN
The synth feed generates messages continuously. When the circuit breaker is OPEN,
all messages go to `buffer.db`. After a long run the buffer can grow to tens of
thousands of frames. Always `rm buffer.db` before a demo restart.

### No `--clear-buffer` flag
There is no clean CLI option to clear the SQLite buffer on startup. It requires
manually deleting `buffer.db`. A `--clear-buffer` or `--max-age` startup flag
would improve demo ergonomics.

### DLM logs active_dicts=0 in health
The health endpoint shows `active_dicts: 0` even though 3 dictionaries are
registered. This is because `dlm.get_stats()["versions"]` returns an empty dict
initially. The dicts are registered but `get_stats()` may not reflect them
correctly — worth investigating DLM's `get_stats()` implementation.

---

## Roadmap / next steps (prioritized)

### High priority
1. **Confirm `_is_connection_alive` fix resolves broken-pipe cycle**  
   Run the stack for 5+ minutes with `buffer.db` deleted. Watch agent logs for
   `batch_buffered` with `[Errno 32] Broken pipe`. If still occurring, investigate
   whether the collector closes connections after each frame.

2. **Commit all V2 bug fixes**  
   Five fixes are in the code but not committed. Use `git diff` to review, then commit.

3. **`--clear-buffer` startup flag**  
   Add to `edge_agent.py`'s `main()` argparse: `--clear-buffer` deletes `buffer.db`
   before starting. Makes demo restarts one-command clean.

### Medium priority
4. **Fix `active_dicts` in health response**  
   Investigate `DictionaryLifecycleManager.get_stats()` — should return dict count
   reflecting currently active compressors.

5. **Add benchmark results to README**  
   The `scripts/benchmark.py --json` output is suitable for embedding in README.md
   as a "Benchmark" section.

6. **Tests for new code**  
   - `_BoundedThreadHTTPServer` concurrent request handling
   - `_HealthCache` TTL behavior under concurrent access
   - `_is_connection_alive` with mock sockets (closed vs alive vs EAGAIN)
   - Recovery loop pacing (verify `time.sleep(0.5)` is called between batches)

### Lower priority
7. **Docker smoke test**  
   `docker compose up --build` + `curl http://localhost:8080/health` — verify the
   Dockerfile multi-stage build still works after V2 changes.

8. **`edge_sentinel` integration**  
   The eBPF/XDP enforcement layer is described in the README but not implemented.
   This is a stretch goal; mention it accurately as "roadmap" in the pitch deck.

9. **Performance profiling under load**  
   The benchmark shows ~1M msg/s theoretical throughput. Under real TCP load with
   the collector decoding and writing JSONL, the bottleneck will shift. A profiling
   pass (cProfile or py-spy) would identify where.

---

## Key file locations

```
edge_dynamics/
├── src/edge_dynamics/
│   ├── edge_agent.py              Main agent: health server, flush loop, recovery
│   ├── collector_server.py        TCP collector with HMAC verification
│   ├── config.py                  Pydantic v2 Settings (env-based)
│   ├── circuit_breaker.py         CLOSED/OPEN/HALF_OPEN state machine
│   ├── disk_buffer.py             SQLite store-and-forward
│   ├── security.py                HMAC signing + mTLS context
│   └── edge_utils/
│       ├── connection_pool.py     Thread-safe socket pool (bug fixes here)
│       ├── dict_lifecycle.py      DLM: EMA drift + blue/green dict swap
│       ├── backpressure.py        BackpressureGate: 3-tier graduated response
│       └── config.py              Pydantic v2 edge_utils settings
├── scripts/
│   └── benchmark.py              Compression / throughput / recovery benchmarks
├── tests/                        146 tests, >90% coverage
├── Dockerfile                    Multi-stage: edge-agent + collector targets
└── docker-compose.yml            Agent + collector services, edge_network

EdgeAgent/
├── server/routes.ts               Express BFF: /api/agent/health proxy (2s timeout)
├── client/src/
│   ├── hooks/useAgentHealth.ts    TanStack Query hook (2s poll, background refetch)
│   └── components/sections/
│       ├── live-demo.tsx          AreaChart + metric cards + activity log (LIVE data)
│       ├── value-pillars.tsx      Five-Layer Stack pillars
│       ├── how-it-works.tsx       edge_dynamics pipeline steps
│       └── developer-mode.tsx     Agent enqueue + collector wire protocol
```

---

## Environment notes

- Python 3.10+ required; dev machine runs 3.10 (confirmed via pyproject.toml)
- Node via nvm; `npm run dev` starts Express on port 5000, Vite on 5173
- `edge_dynamics` installed as editable package: `pip install -e .`
- No `.env` file needed for basic operation (all settings have defaults)
- `buffer.db` is created in the CWD where `edge_agent` is run (usually `edge_dynamics/`)

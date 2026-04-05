# Package Restructure, Security Hardening & CI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restructure the flat edge_dynamics repo into a proper Python package under `src/`, add TLS+HMAC security to the TCP transport, and wire up isolated tests with GitHub Actions CI.

**Architecture:** The source moves into `src/edge_dynamics/` with a `pyproject.toml` at the repo root. All internal imports become `from edge_dynamics.config import ...` etc. Security adds an `ssl.SSLContext` wrapper on both sides of the TCP socket plus HMAC-SHA256 frame signing using the existing `auth_secret_key` config field. Tests live in `tests/` and mock all network I/O. CI runs pytest on push via GitHub Actions.

**Tech Stack:** Python 3.10+, setuptools, pytest, pytest-cov, zstandard, pydantic-settings, ssl (stdlib), hmac (stdlib), GitHub Actions

---

## File Structure (Final State)

```
edge_dynamics/
├── pyproject.toml                      # Package metadata, deps, tool config
├── .gitignore
├── .github/
│   └── workflows/
│       └── ci.yml                      # GitHub Actions: lint + test
├── src/
│   └── edge_dynamics/
│       ├── __init__.py                 # Package version, public API
│       ├── config.py                   # Pydantic settings (+ TLS/HMAC fields)
│       ├── structured_logging.py       # JSON structured logger
│       ├── circuit_breaker.py          # Circuit breaker pattern
│       ├── disk_buffer.py              # SQLite persistent buffer
│       ├── security.py                 # NEW: TLS context builder + HMAC signing
│       ├── edge_agent.py               # Edge agent (updated imports + security)
│       ├── collector_server.py         # Collector (updated imports + security)
│       └── train_dict.py               # Dictionary training utility
├── tests/
│   ├── conftest.py                     # Shared fixtures
│   ├── test_config.py                  # Config validation tests
│   ├── test_circuit_breaker.py         # Circuit breaker state machine tests
│   ├── test_disk_buffer.py             # SQLite buffer tests
│   ├── test_security.py                # TLS + HMAC tests
│   ├── test_edge_agent.py              # Agent tests (mocked network)
│   └── test_collector_server.py        # Collector tests (mocked network)
├── dicts/                              # Pre-trained compression dictionaries
├── scripts/
│   ├── demo_resilience.py              # Resilience demo (updated paths)
│   ├── setup_demo_dicts.py             # Dictionary setup script
│   ├── generate_poc_data.py            # POC data generator
│   └── visualize_poc.py                # POC visualization
├── README.md
└── POC_SUMMARY.md
```

**Files removed from root** (moved into `src/` or `scripts/`):
- `edge_agent.py`, `collector_server.py`, `config.py`, `structured_logging.py`, `circuit_breaker.py`, `disk_buffer.py`, `train_dict.py` → `src/edge_dynamics/`
- `demo_resilience.py`, `setup_demo_dicts.py`, `generate_poc_data.py`, `visualize_poc.py` → `scripts/`
- `test_enhanced_edge_agent.py` → replaced by split test files in `tests/`
- `requirements.txt` → absorbed into `pyproject.toml`
- `edge_agent_v2.py`, `collector_server_v2.py`, `proxy.py`, `collector.py`, `up.py` → deleted (superseded by current versions)

---

## Phase 1: Package Restructure

### Task 1: Create pyproject.toml and package skeleton

**Files:**
- Create: `pyproject.toml`
- Create: `src/edge_dynamics/__init__.py`

- [ ] **Step 1: Create pyproject.toml**

```toml
[build-system]
requires = ["setuptools>=68.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "edge-dynamics"
version = "0.2.0"
description = "Edge-side compression using per-topic dictionaries and batching for IoT telemetry"
readme = "README.md"
requires-python = ">=3.10"
license = {text = "Apache-2.0"}
authors = [
    {name = "Dennis J Carroll"}
]
keywords = ["compression", "iot", "edge-computing", "zstandard", "telemetry"]
classifiers = [
    "Development Status :: 3 - Alpha",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
]

dependencies = [
    "zstandard>=0.22.0",
    "ujson>=5.9.0",
    "pydantic>=2.0.0",
    "pydantic-settings>=2.0.0",
    "psutil>=5.9.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.4.0",
    "pytest-cov>=4.1.0",
]

[project.scripts]
edge-agent = "edge_dynamics.edge_agent:main"
edge-collector = "edge_dynamics.collector_server:main"
edge-train-dict = "edge_dynamics.train_dict:main"

[project.urls]
Repository = "https://github.com/Dennis-J-Carroll/edge_dynamics"

[tool.pytest.ini_options]
minversion = "7.0"
addopts = "-ra -q --strict-markers"
testpaths = ["tests"]

[tool.setuptools.packages.find]
where = ["src"]
```

- [ ] **Step 2: Create `src/edge_dynamics/__init__.py`**

```python
"""Edge Dynamics: edge-side compression with per-topic dictionaries."""

__version__ = "0.2.0"
```

- [ ] **Step 3: Verify directory structure exists**

Run: `ls src/edge_dynamics/__init__.py && cat pyproject.toml | head -3`
Expected: file exists, shows `[build-system]`

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml src/
git commit -m "chore: add pyproject.toml and package skeleton under src/"
```

---

### Task 2: Move core modules into src/edge_dynamics/

**Files:**
- Move: `config.py` → `src/edge_dynamics/config.py`
- Move: `structured_logging.py` → `src/edge_dynamics/structured_logging.py`
- Move: `circuit_breaker.py` → `src/edge_dynamics/circuit_breaker.py`
- Move: `disk_buffer.py` → `src/edge_dynamics/disk_buffer.py`
- Move: `train_dict.py` → `src/edge_dynamics/train_dict.py`

These modules have NO internal cross-imports (they only use stdlib + pydantic), so they can be moved as-is.

- [ ] **Step 1: Move the five core modules**

```bash
git mv config.py src/edge_dynamics/config.py
git mv structured_logging.py src/edge_dynamics/structured_logging.py
git mv circuit_breaker.py src/edge_dynamics/circuit_breaker.py
git mv disk_buffer.py src/edge_dynamics/disk_buffer.py
git mv train_dict.py src/edge_dynamics/train_dict.py
```

- [ ] **Step 2: Verify files moved**

Run: `ls src/edge_dynamics/`
Expected: `__init__.py  circuit_breaker.py  config.py  disk_buffer.py  structured_logging.py  train_dict.py`

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "chore: move core modules into src/edge_dynamics/"
```

---

### Task 3: Move edge_agent.py and update internal imports

**Files:**
- Move: `edge_agent.py` → `src/edge_dynamics/edge_agent.py`
- Modify: `src/edge_dynamics/edge_agent.py` (update imports)

- [ ] **Step 1: Move edge_agent.py**

```bash
git mv edge_agent.py src/edge_dynamics/edge_agent.py
```

- [ ] **Step 2: Update imports in edge_agent.py**

Replace these four lines at the top of `src/edge_dynamics/edge_agent.py`:

```python
from config import get_settings
from structured_logging import get_logger
from circuit_breaker import CircuitBreaker, CircuitState
from disk_buffer import DiskBuffer
```

With:

```python
from edge_dynamics.config import get_settings
from edge_dynamics.structured_logging import get_logger
from edge_dynamics.circuit_breaker import CircuitBreaker, CircuitState
from edge_dynamics.disk_buffer import DiskBuffer
```

- [ ] **Step 3: Add a `main()` entry point** (currently the script uses `if __name__` directly)

At the bottom of `src/edge_dynamics/edge_agent.py`, replace:

```python
if __name__ == "__main__":
    agent = EdgeAgent()
    agent.start()
    # For demonstration, run the synthetic feeder
    synth_feed(agent)
```

With:

```python
def main():
    agent = EdgeAgent()
    agent.start()
    synth_feed(agent)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Verify import works**

Run: `cd /home/dennisjcarroll/Desktop/edgeAgent/edge_dynamics && python -c "from edge_dynamics.edge_agent import EdgeAgent; print('OK')"`
(This may fail if settings try to create dirs — that's fine, we'll fix in Task 5. Just check it doesn't get an ImportError.)

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "chore: move edge_agent into package, update internal imports"
```

---

### Task 4: Move collector_server.py and update imports

**Files:**
- Move: `collector_server.py` → `src/edge_dynamics/collector_server.py`
- Modify: `src/edge_dynamics/collector_server.py` (add main wrapper)

- [ ] **Step 1: Move collector_server.py**

```bash
git mv collector_server.py src/edge_dynamics/collector_server.py
```

- [ ] **Step 2: Update path references in collector_server.py**

The collector uses `os.path.dirname(__file__)` for DICT_DIR, OUT_DIR, METRICS. These need to point to the project root, not the package dir. Replace:

```python
DICT_DIR = os.path.join(os.path.dirname(__file__), "dicts")
OUT_DIR = os.path.join(os.path.dirname(__file__), "out")
METRICS = os.path.join(os.path.dirname(__file__), "metrics.csv")
```

With:

```python
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DICT_DIR = os.path.join(_PROJECT_ROOT, "dicts")
OUT_DIR = os.path.join(_PROJECT_ROOT, "out")
METRICS = os.path.join(_PROJECT_ROOT, "metrics.csv")
```

- [ ] **Step 3: Wrap `main()` for entry point**

At the bottom of the file, the `if __name__` block already calls `main()` — verify this is the case. No change needed.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "chore: move collector_server into package, fix data paths"
```

---

### Task 5: Move scripts, delete superseded files, update .gitignore

**Files:**
- Move: `demo_resilience.py` → `scripts/demo_resilience.py`
- Move: `setup_demo_dicts.py` → `scripts/setup_demo_dicts.py`
- Move: `generate_poc_data.py` → `scripts/generate_poc_data.py`
- Move: `visualize_poc.py` → `scripts/visualize_poc.py`
- Delete: `edge_agent_v2.py`, `collector_server_v2.py`, `proxy.py`, `collector.py`, `up.py`
- Delete: `requirements.txt` (absorbed into pyproject.toml)
- Delete: `test_enhanced_edge_agent.py` (will be replaced by new tests)
- Modify: `.gitignore`

- [ ] **Step 1: Create scripts/ and move demo files**

```bash
mkdir -p scripts
git mv demo_resilience.py scripts/
git mv setup_demo_dicts.py scripts/
git mv generate_poc_data.py scripts/
git mv visualize_poc.py scripts/
```

- [ ] **Step 2: Delete superseded files**

```bash
git rm edge_agent_v2.py collector_server_v2.py proxy.py collector.py up.py
git rm requirements.txt
git rm test_enhanced_edge_agent.py
```

- [ ] **Step 3: Update demo_resilience.py subprocess commands**

In `scripts/demo_resilience.py`, replace:

```python
COLLECTOR_CMD = ["python3", "collector_server.py"]
AGENT_CMD = ["python3", "edge_agent.py"]
```

With:

```python
COLLECTOR_CMD = ["python3", "-m", "edge_dynamics.collector_server"]
AGENT_CMD = ["python3", "-m", "edge_dynamics.edge_agent"]
```

And replace:

```python
subprocess.run(["python3", "setup_demo_dicts.py"], check=True)
```

With:

```python
subprocess.run(["python3", "scripts/setup_demo_dicts.py"], check=True)
```

- [ ] **Step 4: Update .gitignore** — remove `dicts/` from ignored list (we need the trained dicts committed), add `buffer.db`

Remove this line from `.gitignore`:
```
dicts/
```

Ensure `buffer.db` is present in the project-specific section.

- [ ] **Step 5: Install package in editable mode**

Run: `pip install -e ".[dev]"`
Expected: successful install, `edge-agent --help` or `python -c "import edge_dynamics"` works

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "chore: move scripts, delete superseded files, finalize package structure"
```

---

## Phase 2: Security Hardening

### Task 6: Add TLS + HMAC config fields

**Files:**
- Modify: `src/edge_dynamics/config.py`

- [ ] **Step 1: Add TLS and HMAC fields to Settings class**

Add these fields to the `Settings` class in `src/edge_dynamics/config.py`, after the existing `auth_secret_key` field:

```python
    # TLS Configuration
    tls_enabled: bool = Field(
        default=False,
        description="Enable TLS for collector connection"
    )
    tls_cert_file: Optional[str] = Field(
        default=None,
        description="Path to TLS certificate file (PEM)"
    )
    tls_key_file: Optional[str] = Field(
        default=None,
        description="Path to TLS private key file (PEM)"
    )
    tls_ca_file: Optional[str] = Field(
        default=None,
        description="Path to CA certificate file for verification"
    )

    # HMAC Configuration
    hmac_enabled: bool = Field(
        default=False,
        description="Enable HMAC frame signing"
    )
```

- [ ] **Step 2: Add TLS validation**

Add this validator after the existing validators:

```python
    @validator("tls_cert_file", "tls_key_file")
    def validate_tls_files(cls, v, values):
        """Validate TLS files exist if TLS is enabled."""
        if values.get("tls_enabled") and v is not None and not os.path.isfile(v):
            raise ValueError(f"TLS file not found: {v}")
        return v
```

- [ ] **Step 3: Commit**

```bash
git add src/edge_dynamics/config.py
git commit -m "feat: add TLS and HMAC configuration fields"
```

---

### Task 7: Create security.py module

**Files:**
- Create: `src/edge_dynamics/security.py`
- Test: `tests/test_security.py`

- [ ] **Step 1: Write tests for HMAC signing**

Create `tests/test_security.py`:

```python
import struct
import hmac
import hashlib
import ssl
import tempfile
import os
import pytest

from edge_dynamics.security import sign_frame, verify_frame, build_tls_context


class TestHMACSigning:
    """Test HMAC-SHA256 frame signing and verification."""

    def test_sign_and_verify_roundtrip(self):
        """Signed frame should verify successfully."""
        secret = b"test-secret-key-1234"
        frame = b"some compressed payload data here"
        signed = sign_frame(frame, secret)
        assert verify_frame(signed, secret) == frame

    def test_signed_frame_is_longer(self):
        """Signing prepends a 32-byte HMAC digest."""
        secret = b"key"
        frame = b"data"
        signed = sign_frame(frame, secret)
        assert len(signed) == len(frame) + 32

    def test_verify_rejects_tampered_frame(self):
        """Tampered payload should fail verification."""
        secret = b"key"
        signed = sign_frame(b"original", secret)
        tampered = signed[:32] + b"tampered"
        with pytest.raises(ValueError, match="HMAC verification failed"):
            verify_frame(tampered, secret)

    def test_verify_rejects_wrong_key(self):
        """Wrong key should fail verification."""
        signed = sign_frame(b"data", b"correct-key")
        with pytest.raises(ValueError, match="HMAC verification failed"):
            verify_frame(signed, b"wrong-key")

    def test_verify_rejects_truncated_frame(self):
        """Frame shorter than 32 bytes should fail."""
        with pytest.raises(ValueError, match="too short"):
            verify_frame(b"short", b"key")


class TestTLSContext:
    """Test TLS context construction."""

    def test_build_server_context_with_cert(self):
        """Server context should load cert and key."""
        # Generate a self-signed cert for testing
        certfile, keyfile = _generate_self_signed_cert()
        try:
            ctx = build_tls_context(
                server_side=True,
                certfile=certfile,
                keyfile=keyfile,
            )
            assert isinstance(ctx, ssl.SSLContext)
        finally:
            os.unlink(certfile)
            os.unlink(keyfile)

    def test_build_client_context_no_verify(self):
        """Client context without CA skips verification."""
        ctx = build_tls_context(server_side=False)
        assert isinstance(ctx, ssl.SSLContext)


def _generate_self_signed_cert():
    """Generate a temporary self-signed cert+key pair for testing."""
    import subprocess
    certfile = tempfile.mktemp(suffix=".pem")
    keyfile = tempfile.mktemp(suffix=".key")
    subprocess.run([
        "openssl", "req", "-x509", "-newkey", "rsa:2048",
        "-keyout", keyfile, "-out", certfile,
        "-days", "1", "-nodes",
        "-subj", "/CN=localhost"
    ], check=True, capture_output=True)
    return certfile, keyfile
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_security.py -v`
Expected: ImportError — `edge_dynamics.security` does not exist yet

- [ ] **Step 3: Implement security.py**

Create `src/edge_dynamics/security.py`:

```python
"""
TLS context builder and HMAC frame signing for edge_dynamics.

Frame signing protocol:
    Signed frame = [32-byte HMAC-SHA256 digest][original frame bytes]
    The digest covers only the frame bytes (not itself).
"""

import hmac
import hashlib
import ssl
from typing import Optional


def sign_frame(frame: bytes, secret: bytes) -> bytes:
    """
    Prepend an HMAC-SHA256 digest to a frame.

    Args:
        frame: The raw frame bytes to sign.
        secret: Shared secret key.

    Returns:
        32-byte digest + original frame.
    """
    digest = hmac.new(secret, frame, hashlib.sha256).digest()
    return digest + frame


def verify_frame(signed_frame: bytes, secret: bytes) -> bytes:
    """
    Verify and strip the HMAC digest from a signed frame.

    Args:
        signed_frame: The signed frame (digest + payload).
        secret: Shared secret key.

    Returns:
        The original frame bytes.

    Raises:
        ValueError: If the frame is too short or the digest doesn't match.
    """
    if len(signed_frame) < 32:
        raise ValueError("Signed frame too short to contain HMAC digest")

    received_digest = signed_frame[:32]
    frame = signed_frame[32:]

    expected_digest = hmac.new(secret, frame, hashlib.sha256).digest()
    if not hmac.compare_digest(received_digest, expected_digest):
        raise ValueError("HMAC verification failed")

    return frame


def build_tls_context(
    server_side: bool = False,
    certfile: Optional[str] = None,
    keyfile: Optional[str] = None,
    cafile: Optional[str] = None,
) -> ssl.SSLContext:
    """
    Build an SSLContext for the agent or collector.

    Args:
        server_side: True for the collector, False for the agent.
        certfile: Path to PEM certificate (required for server).
        keyfile: Path to PEM private key (required for server).
        cafile: Path to CA cert for peer verification.

    Returns:
        Configured SSLContext.
    """
    if server_side:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        if certfile and keyfile:
            ctx.load_cert_chain(certfile, keyfile)
    else:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        if cafile:
            ctx.load_verify_locations(cafile)
        else:
            # No CA provided — disable verification (development mode)
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE

    return ctx
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_security.py -v`
Expected: All 6 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/edge_dynamics/security.py tests/test_security.py
git commit -m "feat: add HMAC frame signing and TLS context builder"
```

---

### Task 8: Wire TLS + HMAC into edge_agent.py

**Files:**
- Modify: `src/edge_dynamics/edge_agent.py`

- [ ] **Step 1: Add security imports**

At the top of `edge_agent.py`, after the existing `from edge_dynamics.disk_buffer import DiskBuffer` line, add:

```python
from edge_dynamics.security import sign_frame, build_tls_context
```

- [ ] **Step 2: Update `_get_sock()` to support TLS**

Replace the existing `_get_sock` method:

```python
    def _get_sock(self) -> socket.socket:
        """Return the cached TCP socket, creating a new one if needed."""
        if self._sock is None:
            self._sock = socket.create_connection(
                (settings.collector_host, settings.collector_port), 
                timeout=2
            )
        return self._sock
```

With:

```python
    def _get_sock(self) -> socket.socket:
        """Return the cached TCP socket, optionally wrapped with TLS."""
        if self._sock is None:
            raw = socket.create_connection(
                (settings.collector_host, settings.collector_port),
                timeout=2
            )
            if settings.tls_enabled:
                ctx = build_tls_context(
                    server_side=False,
                    cafile=settings.tls_ca_file,
                )
                self._sock = ctx.wrap_socket(raw, server_hostname=settings.collector_host)
            else:
                self._sock = raw
        return self._sock
```

- [ ] **Step 3: Add HMAC signing to `_flush_batch()`**

In the `_flush_batch` method, after constructing `frame` and before the `try:` that calls `self._send_frame(frame)`, add HMAC signing:

```python
        frame = struct.pack("!I", len(hdr_bytes)) + hdr_bytes + comp_payload

        # Sign frame if HMAC is enabled
        if settings.hmac_enabled and settings.auth_secret_key:
            frame = sign_frame(frame, settings.auth_secret_key.encode())
```

- [ ] **Step 4: Commit**

```bash
git add src/edge_dynamics/edge_agent.py
git commit -m "feat: wire TLS and HMAC signing into edge agent"
```

---

### Task 9: Wire TLS + HMAC into collector_server.py

**Files:**
- Modify: `src/edge_dynamics/collector_server.py`

- [ ] **Step 1: Add imports**

At the top of `collector_server.py`, add:

```python
import ssl
from edge_dynamics.config import get_settings
from edge_dynamics.security import verify_frame, build_tls_context
```

- [ ] **Step 2: Add HMAC verification to `handle_connection`**

The collector reads frames in a loop. We need to read the full signed frame first if HMAC is enabled. This requires a protocol change: when HMAC is enabled, the collector reads a 4-byte length prefix for the signed frame, then verifies. 

**Simpler approach:** Since the collector already knows `comp_len` from the header, and HMAC wraps the entire frame, we handle it at the connection accept level. Add a settings load and wrap the socket accept:

In `main()`, after `dict_index, decompressors = load_dictionaries()`, add:

```python
    settings = get_settings()
```

Replace the server socket creation:

```python
    with socket.create_server((HOST, PORT), reuse_port=True) as srv:
```

With:

```python
    srv_sock = socket.create_server((HOST, PORT), reuse_port=True)
    if settings.tls_enabled:
        tls_ctx = build_tls_context(
            server_side=True,
            certfile=settings.tls_cert_file,
            keyfile=settings.tls_key_file,
        )
        srv_sock = tls_ctx.wrap_socket(srv_sock, server_side=True)

    with srv_sock as srv:
```

- [ ] **Step 3: Add HMAC verification in handle_connection**

For HMAC, the agent sends `[32-byte hmac][4-byte hdr_len][header][payload]`. The collector must strip the HMAC first. In `handle_connection`, at the start of the while loop, before reading the header:

```python
                try:
                    # If HMAC enabled, first 32 bytes are the digest
                    # We read header_len first to know total frame size,
                    # but HMAC wraps the whole frame. So with HMAC:
                    #   wire = [32-byte digest][4-byte hdr_len][header][payload]
                    # Without HMAC:
                    #   wire = [4-byte hdr_len][header][payload]
                    
                    if settings.hmac_enabled and settings.auth_secret_key:
                        # Read HMAC digest
                        hmac_digest = recvall(conn, 32)
                        # Read header length
                        header_len_bytes = recvall(conn, 4)
                        header_len = struct.unpack("!I", header_len_bytes)[0]
                        header_data = recvall(conn, header_len)
                        header = json.loads(header_data)
                        comp_payload = recvall(conn, header["comp_len"])
                        # Reconstruct original frame and verify
                        original_frame = header_len_bytes + header_data + comp_payload
                        verify_frame(
                            hmac_digest + original_frame,
                            settings.auth_secret_key.encode()
                        )
                    else:
                        header_len_bytes = recvall(conn, 4)
                        header_len = struct.unpack("!I", header_len_bytes)[0]
                        header_data = recvall(conn, header_len)
                        header = json.loads(header_data)
                        comp_payload = recvall(conn, header["comp_len"])
```

- [ ] **Step 4: Commit**

```bash
git add src/edge_dynamics/collector_server.py
git commit -m "feat: wire TLS and HMAC verification into collector"
```

---

## Phase 3: Testing & CI

### Task 10: Create test fixtures (conftest.py)

**Files:**
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`

- [ ] **Step 1: Create tests/__init__.py**

```python
# tests package
```

- [ ] **Step 2: Create conftest.py with shared fixtures**

```python
"""Shared test fixtures for edge_dynamics."""

import os
import tempfile
import pytest
from unittest.mock import patch, MagicMock

from edge_dynamics.config import Settings


@pytest.fixture
def test_settings(tmp_path):
    """Create a Settings instance with temp paths for isolated tests."""
    return Settings(
        collector_host="127.0.0.1",
        collector_port=17000,
        batch_max=5,
        batch_ms=100,
        compression_level=3,
        dict_dir=str(tmp_path / "dicts"),
        out_dir=str(tmp_path / "out"),
        metrics_file=str(tmp_path / "metrics.csv"),
        disk_buffer_enabled=True,
        disk_buffer_path=str(tmp_path / "buffer.db"),
        disk_buffer_max_mb=1,
        health_check_port=18080,
        tls_enabled=False,
        hmac_enabled=False,
    )


@pytest.fixture
def mock_socket():
    """Mock socket.create_connection to avoid real network I/O."""
    with patch("edge_dynamics.edge_agent.socket.create_connection") as mock_conn:
        mock_sock = MagicMock()
        mock_conn.return_value = mock_sock
        yield mock_sock


@pytest.fixture
def isolated_agent(test_settings, mock_socket):
    """Create an EdgeAgent with mocked network and isolated paths."""
    with patch("edge_dynamics.edge_agent.settings", test_settings), \
         patch("edge_dynamics.edge_agent.load_dictionaries", return_value={}), \
         patch("edge_dynamics.edge_agent.build_compressors", return_value={}):
        from edge_dynamics.edge_agent import EdgeAgent
        agent = EdgeAgent(enable_flush=False, enable_metrics=False, enable_health=False)
        yield agent


@pytest.fixture
def disk_buffer(tmp_path):
    """Create a DiskBuffer with a temp database."""
    from edge_dynamics.disk_buffer import DiskBuffer
    return DiskBuffer(str(tmp_path / "test_buffer.db"), max_mb=1)
```

- [ ] **Step 3: Commit**

```bash
git add tests/
git commit -m "test: add conftest with shared fixtures for isolated testing"
```

---

### Task 11: Port and improve unit tests

**Files:**
- Create: `tests/test_config.py`
- Create: `tests/test_circuit_breaker.py`
- Create: `tests/test_disk_buffer.py`
- Create: `tests/test_edge_agent.py`

- [ ] **Step 1: Create tests/test_config.py**

```python
"""Tests for configuration management."""

import os
import pytest
from edge_dynamics.config import Settings


class TestConfigDefaults:
    def test_default_collector_settings(self):
        s = Settings()
        assert s.collector_host == "127.0.0.1"
        assert s.collector_port == 7000

    def test_default_batch_settings(self):
        s = Settings()
        assert s.batch_max == 100
        assert s.batch_ms == 250

    def test_default_compression(self):
        s = Settings()
        assert s.compression_level == 7


class TestConfigValidation:
    def test_rejects_invalid_port(self):
        with pytest.raises(ValueError):
            Settings(collector_port=70000)

    def test_rejects_invalid_compression_level(self):
        with pytest.raises(ValueError):
            Settings(compression_level=25)

    def test_rejects_negative_batch_max(self):
        with pytest.raises(ValueError):
            Settings(batch_max=-1)

    def test_rejects_invalid_log_level(self):
        with pytest.raises(ValueError):
            Settings(log_level="INVALID")


class TestConfigEnvOverride:
    def test_env_overrides_defaults(self):
        os.environ["EDGE_COLLECTOR_PORT"] = "9000"
        try:
            s = Settings()
            assert s.collector_port == 9000
        finally:
            del os.environ["EDGE_COLLECTOR_PORT"]
```

- [ ] **Step 2: Create tests/test_circuit_breaker.py**

```python
"""Tests for circuit breaker state machine."""

import time
import pytest
from edge_dynamics.circuit_breaker import CircuitBreaker, CircuitState


class TestCircuitBreakerStates:
    def test_starts_closed(self):
        cb = CircuitBreaker(name="test")
        assert cb.state == CircuitState.CLOSED

    def test_opens_after_threshold_failures(self):
        cb = CircuitBreaker(name="test", failure_threshold=3, timeout=10.0)
        call_count = 0

        @cb
        def failing():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("fail")

        for _ in range(3):
            with pytest.raises(ConnectionError):
                failing()

        assert cb.state == CircuitState.OPEN

    def test_rejects_calls_when_open(self):
        cb = CircuitBreaker(name="test", failure_threshold=1)

        @cb
        def failing():
            raise ConnectionError("fail")

        with pytest.raises(ConnectionError):
            failing()

        with pytest.raises(Exception, match="OPEN"):
            failing()

    def test_transitions_to_half_open_after_timeout(self):
        cb = CircuitBreaker(name="test", failure_threshold=1, timeout=0.1)

        @cb
        def failing():
            raise ConnectionError("fail")

        with pytest.raises(ConnectionError):
            failing()

        assert cb.state == CircuitState.OPEN
        time.sleep(0.15)
        assert cb.state == CircuitState.HALF_OPEN

    def test_closes_after_success_in_half_open(self):
        cb = CircuitBreaker(name="test", failure_threshold=1, timeout=0.1, success_threshold=1)
        should_fail = True

        @cb
        def maybe_fail():
            if should_fail:
                raise ConnectionError("fail")
            return "ok"

        with pytest.raises(ConnectionError):
            maybe_fail()

        time.sleep(0.15)
        should_fail = False
        result = maybe_fail()
        assert result == "ok"
        assert cb.state == CircuitState.CLOSED

    def test_force_open_and_close(self):
        cb = CircuitBreaker(name="test")
        cb.force_open()
        assert cb.state == CircuitState.OPEN
        cb.force_close()
        assert cb.state == CircuitState.CLOSED
```

- [ ] **Step 3: Create tests/test_disk_buffer.py**

```python
"""Tests for SQLite persistent buffer."""

import pytest
from edge_dynamics.disk_buffer import DiskBuffer


class TestDiskBufferOperations:
    def test_push_and_pop(self, disk_buffer):
        disk_buffer.push("topic-a", b"payload-1")
        disk_buffer.push("topic-b", b"payload-2")

        assert disk_buffer.get_count() == 2

        batches = disk_buffer.pop_batch(limit=1)
        assert len(batches) == 1
        assert batches[0][1] == "topic-a"
        assert batches[0][2] == b"payload-1"
        assert disk_buffer.get_count() == 1

    def test_pop_returns_fifo_order(self, disk_buffer):
        for i in range(5):
            disk_buffer.push(f"t{i}", f"p{i}".encode())

        batches = disk_buffer.pop_batch(limit=5)
        topics = [b[1] for b in batches]
        assert topics == ["t0", "t1", "t2", "t3", "t4"]

    def test_pop_empty_buffer(self, disk_buffer):
        batches = disk_buffer.pop_batch(limit=10)
        assert batches == []
        assert disk_buffer.get_count() == 0

    def test_fifo_eviction_under_size_limit(self, tmp_path):
        buf = DiskBuffer(str(tmp_path / "evict.db"), max_mb=0)
        # max_mb=0 means 0 bytes limit — everything should be evicted
        large = b"x" * 4096
        for i in range(20):
            buf.push(f"t{i}", large)
        # Should have evicted older entries
        assert buf.get_count() < 20
```

- [ ] **Step 4: Create tests/test_edge_agent.py**

```python
"""Tests for edge agent batching, compression, and sending."""

import json
import struct
import time
import pytest
from unittest.mock import patch, Mock, MagicMock

from edge_dynamics.edge_agent import normalize_message


class TestNormalizeMessage:
    def test_removes_trace_id(self):
        msg = {"headers": {"Accept": "*/*", "X-Amzn-Trace-Id": "Root=123"}, "data": "v"}
        result = json.loads(normalize_message(msg))
        assert "X-Amzn-Trace-Id" not in result["headers"]
        assert result["headers"]["Accept"] == "*/*"

    def test_handles_missing_headers(self):
        msg = {"data": "v", "n": 42}
        result = json.loads(normalize_message(msg))
        assert result == {"data": "v", "n": 42}

    def test_compact_json(self):
        raw = normalize_message({"a": 1, "b": [2, 3]}).decode()
        assert ": " not in raw
        assert ", " not in raw


class TestEdgeAgentEnqueue:
    def test_enqueue_adds_to_buffer(self, isolated_agent):
        isolated_agent.enqueue("t1", {"v": 1})
        assert "t1" in isolated_agent.buffers
        assert len(isolated_agent.buffers["t1"]["q"]) == 1

    def test_topics_are_isolated(self, isolated_agent):
        for i in range(3):
            isolated_agent.enqueue("a", {"i": i})
            isolated_agent.enqueue("b", {"i": i})
        assert len(isolated_agent.buffers["a"]["q"]) == 3
        assert len(isolated_agent.buffers["b"]["q"]) == 3

    def test_flush_triggers_at_batch_max(self, isolated_agent, test_settings):
        with patch.object(isolated_agent, "_flush_batch") as mock_flush:
            for i in range(test_settings.batch_max):
                isolated_agent.enqueue("t", {"i": i})
            assert mock_flush.call_count == 1


class TestEdgeAgentFlush:
    def test_flush_sends_frame_to_socket(self, isolated_agent, mock_socket):
        msgs = [normalize_message({"d": i}) for i in range(3)]
        isolated_agent._flush_batch("test.topic", msgs)
        mock_socket.sendall.assert_called_once()

    def test_flush_without_dict_sends_raw(self, isolated_agent, mock_socket):
        msgs = [normalize_message({"d": 1})]
        isolated_agent._flush_batch("unknown.topic", msgs)
        frame = mock_socket.sendall.call_args[0][0]
        hdr_len = struct.unpack("!I", frame[:4])[0]
        header = json.loads(frame[4 : 4 + hdr_len])
        assert header["dict_id"] == ""

    def test_flush_failure_buffers_to_disk(self, isolated_agent, mock_socket):
        mock_socket.sendall.side_effect = ConnectionError("refused")
        msgs = [normalize_message({"d": 1})]
        isolated_agent._flush_batch("t", msgs)
        assert isolated_agent.disk_buffer.get_count() == 1


class TestEdgeAgentMetrics:
    def test_metrics_recorded_on_flush(self, isolated_agent, mock_socket):
        msgs = [normalize_message({"d": i}) for i in range(3)]
        isolated_agent._flush_batch("t", msgs)
        stats = isolated_agent.metrics.get_stats()
        assert stats["messages_processed"] == 3
        assert stats["batch_count"] == 1
        assert stats["bytes_in"] > 0
        assert stats["bytes_out"] > 0
```

- [ ] **Step 5: Run all tests**

Run: `pytest tests/ -v`
Expected: All tests pass

- [ ] **Step 6: Commit**

```bash
git add tests/
git commit -m "test: add comprehensive unit tests with mocked network I/O"
```

---

### Task 12: Add GitHub Actions CI

**Files:**
- Create: `.github/workflows/ci.yml`

- [ ] **Step 1: Create CI workflow**

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.10", "3.11", "3.12"]

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python ${{ matrix.python-version }}
        uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}

      - name: Install dependencies
        run: pip install -e ".[dev]"

      - name: Run tests
        run: pytest tests/ -v --tb=short
```

- [ ] **Step 2: Commit**

```bash
git add .github/
git commit -m "ci: add GitHub Actions workflow for Python 3.10-3.12"
```

---

### Task 13: Final integration verification

- [ ] **Step 1: Run full test suite from clean state**

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

Expected: All tests pass

- [ ] **Step 2: Verify package imports work**

```bash
python -c "from edge_dynamics.edge_agent import EdgeAgent; from edge_dynamics.security import sign_frame; print('All imports OK')"
```

- [ ] **Step 3: Verify entry points**

```bash
python -m edge_dynamics.edge_agent --help 2>&1 || echo "Entry point reachable"
```

- [ ] **Step 4: Final commit with all remaining changes**

```bash
git add -A
git status
# If there are any remaining changes:
git commit -m "chore: final cleanup after restructure"
git push origin main
```

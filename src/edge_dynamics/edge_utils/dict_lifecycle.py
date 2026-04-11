# SPDX-License-Identifier: Apache-2.0
"""
edge_utils/dict_lifecycle.py

Adaptive Dictionary Lifecycle Manager for edge_dynamics v2.

Two core classes:

    RatioMonitor  — EMA-based compression ratio tracker per topic.
                    Detects schema drift by measuring when the rolling
                    average ratio degrades beyond a threshold relative
                    to the established baseline.

    DictionaryLifecycleManager — Orchestrates dictionary versions.
                    Buffers raw message samples for retraining, triggers
                    background retrains on drift signal, performs blue/green
                    version swaps so in-flight batches always decompress.

─────────────────────────────────────────────────────────
Connection to the Minus One Principle
─────────────────────────────────────────────────────────
The EMA update rule, written explicitly:

    ema_t = ema_{t-1} + α * (ratio_t − ema_{t-1})
                              ↑ this term is a difference

Drift detection:
    drift = ema_t − baseline_ratio        ← another difference

The entire adaptive system is driven by subtraction.
Error signals are differences. Correction is a step in the
direction of that difference, weighted by α. This is gradient
descent on the ratio loss — same mathematical DNA as
backpropagation, Bessel's correction (n−1), and attention scores.
Subtraction is the seed of adaptation.
─────────────────────────────────────────────────────────
"""

import hashlib
import json
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Literal, Optional, Set, Tuple

import zstandard as zstd


# ──────────────────────────────────────────────────────────────────────────────
# Logging shim — uses edge_utils logger if available, falls back to print
# ──────────────────────────────────────────────────────────────────────────────
try:
    from .logging import get_logger
    _log = get_logger("dict_lifecycle")
    def _info(msg, **kw):  _log.info(msg, **kw)
    def _warn(msg, **kw):  _log.warning(msg, **kw)
    def _error(msg, **kw): _log.error(msg, **kw)
    def _debug(msg, **kw): _log.debug(msg, **kw)
except ImportError:
    def _info(msg, **kw):  print(f"[INFO]  {msg}", kw or "")
    def _warn(msg, **kw):  print(f"[WARN]  {msg}", kw or "")
    def _error(msg, **kw): print(f"[ERROR] {msg}", kw or "")
    def _debug(msg, **kw): pass


# ──────────────────────────────────────────────────────────────────────────────
# Data containers
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class RatioEvent:
    """
    Emitted by RatioMonitor on notable state transitions.

    event_type:
        "warmup_complete"  — baseline has been established
        "drift_detected"   — EMA crossed drift_threshold above baseline
        "ratio_update"     — routine update, no state change
        "recovered"        — EMA returned within recovery_threshold of baseline
    """
    topic: str
    event_type: Literal["warmup_complete", "drift_detected", "ratio_update", "recovered"]
    ratio: float          # raw ratio for this batch (comp / raw)
    ema: float            # current EMA value
    baseline: Optional[float]  # None during warmup
    drift_magnitude: float     # ema - baseline (0.0 during warmup)
    batch_count: int
    timestamp: float = field(default_factory=time.time)


@dataclass
class DictionaryVersion:
    """
    One version of a per-topic compression dictionary.

    Multiple versions coexist during blue/green rollout:
    the active version compresses NEW batches; retired versions
    remain alive long enough for in-flight batches to drain.
    """
    dict_id: str
    topic: str
    version: int
    dict_bytes: bytes
    compressor: zstd.ZstdCompressor
    decompressor: zstd.ZstdDecompressor
    created_at: float = field(default_factory=time.time)
    retired_at: Optional[float] = None

    @property
    def is_active(self) -> bool:
        return self.retired_at is None

    @property
    def age_seconds(self) -> float:
        return time.time() - self.created_at

    @property
    def size_bytes(self) -> int:
        return len(self.dict_bytes)


# ──────────────────────────────────────────────────────────────────────────────
# RatioMonitor
# ──────────────────────────────────────────────────────────────────────────────

class RatioMonitor:
    """
    EMA-based compression ratio tracker for a single topic.

    Lifecycle:
        warmup  →  normal  →  drifted
                       ↑___________↓  (recovery resets to normal)

    The EMA is initialized lazily on first update so the monitor
    starts tracking immediately without needing an explicit reset.

    Thread-safe: all state updates are protected by a single RLock.

    Args:
        topic:                 Topic name (for logging / events).
        alpha:                 EMA smoothing factor ∈ (0, 1].
                               Higher = more reactive to recent batches.
                               Lower  = more stable, slower to respond.
                               Rule of thumb: α ≈ 2/(N+1) for N-period EMA.
                               Default 0.10 ≈ a 19-period EMA.
        warmup_batches:        How many batches before baseline is locked in.
                               During warmup, drift detection is disabled.
        drift_threshold:       Relative ratio increase that triggers retrain.
                               0.20 means "alert if EMA is 20% worse than
                               baseline" — e.g., baseline 0.056 → alert at 0.067.
        consecutive_trigger:   How many consecutive drifted-EMA batches must
                               occur before emitting drift_detected.
                               Prevents single-batch anomalies from triggering
                               an expensive retrain.
        recovery_threshold:    Relative improvement vs baseline that constitutes
                               "recovered" (EMA returned to near-baseline).
    """

    def __init__(
        self,
        topic: str,
        alpha: float = 0.10,
        warmup_batches: int = 20,
        drift_threshold: float = 0.20,
        consecutive_trigger: int = 3,
        recovery_threshold: float = 0.05,
    ) -> None:
        self.topic = topic
        self.alpha = alpha
        self.warmup_batches = warmup_batches
        self.drift_threshold = drift_threshold
        self.consecutive_trigger = consecutive_trigger
        self.recovery_threshold = recovery_threshold

        self._ema: Optional[float] = None
        self._baseline: Optional[float] = None
        self._batch_count: int = 0
        self._state: Literal["warmup", "normal", "drifted"] = "warmup"
        self._consecutive_drift: int = 0  # drift batches since last normal
        self._lock = threading.RLock()

        # Instrumentation
        self._peak_ratio: float = 0.0
        self._last_ratio: float = 0.0
        self._retrain_count: int = 0  # how many retrains this monitor has seen

    # ── Public API ────────────────────────────────────────────────────────────

    def update(self, raw_bytes: int, comp_bytes: int) -> RatioEvent:
        """
        Ingest one batch's worth of raw/compressed byte counts.
        Updates EMA and returns a RatioEvent describing the current state.

        The EMA update, spelled out:
            ema_t = ema_{t-1} + α * (ratio_t − ema_{t-1})
        This is a first-order difference equation. α is the learning rate.
        The correction term (ratio_t − ema_{t-1}) is pure subtraction.
        """
        ratio = comp_bytes / max(raw_bytes, 1)

        with self._lock:
            self._batch_count += 1
            self._last_ratio = ratio
            self._peak_ratio = max(self._peak_ratio, ratio)

            # ── EMA update ────────────────────────────────────────────────
            if self._ema is None:
                self._ema = ratio   # cold start: seed with first observation
            else:
                # α * (new − old): the correction is a difference
                self._ema = self._ema + self.alpha * (ratio - self._ema)

            # ── State machine ─────────────────────────────────────────────
            event_type: Literal["warmup_complete", "drift_detected",
                                 "ratio_update", "recovered"] = "ratio_update"

            if self._state == "warmup":
                if self._batch_count >= self.warmup_batches:
                    self._baseline = self._ema
                    self._state = "normal"
                    event_type = "warmup_complete"
                    _info("ratio_baseline_set",
                          topic=self.topic,
                          baseline=round(self._baseline, 6),
                          batch_count=self._batch_count)

            elif self._state == "normal":
                if self._is_drifted():
                    self._consecutive_drift += 1
                    if self._consecutive_drift >= self.consecutive_trigger:
                        self._state = "drifted"
                        event_type = "drift_detected"
                        _warn("schema_drift_detected",
                              topic=self.topic,
                              ema=round(self._ema, 6),
                              baseline=round(self._baseline, 6),
                              drift_pct=round(self._drift_magnitude() * 100, 2))
                else:
                    self._consecutive_drift = 0

            elif self._state == "drifted":
                if self._is_recovered():
                    self._state = "normal"
                    self._consecutive_drift = 0
                    event_type = "recovered"
                    _info("ratio_recovered",
                          topic=self.topic,
                          ema=round(self._ema, 6),
                          baseline=round(self._baseline, 6))

            return RatioEvent(
                topic=self.topic,
                event_type=event_type,
                ratio=ratio,
                ema=self._ema,
                baseline=self._baseline,
                drift_magnitude=self._drift_magnitude(),
                batch_count=self._batch_count,
            )

    def notify_retrained(self) -> None:
        """
        Called by DictionaryLifecycleManager after a successful retrain.
        Resets the baseline to current EMA so the new dictionary's
        performance becomes the new reference point.
        """
        with self._lock:
            self._baseline = self._ema
            self._state = "normal"
            self._consecutive_drift = 0
            self._retrain_count += 1
            _info("ratio_monitor_reset_after_retrain",
                  topic=self.topic,
                  new_baseline=round(self._baseline, 6) if self._baseline else None,
                  retrain_count=self._retrain_count)

    def get_stats(self) -> dict:
        with self._lock:
            return {
                "topic": self.topic,
                "state": self._state,
                "ema": round(self._ema, 6) if self._ema is not None else None,
                "baseline": round(self._baseline, 6) if self._baseline is not None else None,
                "last_ratio": round(self._last_ratio, 6),
                "peak_ratio": round(self._peak_ratio, 6),
                "drift_magnitude": round(self._drift_magnitude(), 6),
                "batch_count": self._batch_count,
                "consecutive_drift": self._consecutive_drift,
                "retrain_count": self._retrain_count,
            }

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _drift_magnitude(self) -> float:
        """
        ema − baseline: the raw difference signal.
        Positive = worse compression (higher ratio = less compressed).
        This IS subtraction as the seed of detection.
        """
        if self._baseline is None or self._ema is None:
            return 0.0
        return self._ema - self._baseline

    def _is_drifted(self) -> bool:
        if self._baseline is None or self._ema is None:
            return False
        # relative drift: how many % worse than baseline?
        relative = self._drift_magnitude() / max(self._baseline, 1e-9)
        return relative > self.drift_threshold

    def _is_recovered(self) -> bool:
        if self._baseline is None or self._ema is None:
            return False
        relative = self._drift_magnitude() / max(self._baseline, 1e-9)
        return relative <= self.recovery_threshold


# ──────────────────────────────────────────────────────────────────────────────
# SampleBuffer
# ──────────────────────────────────────────────────────────────────────────────

class SampleBuffer:
    """
    Thread-safe rolling window of raw (pre-compression) message bytes.

    Maintained per topic. Used as training data when a retrain is triggered.

    Zstandard's dictionary training recommends ≥200× the target dict size
    in sample data for good quality. Default: 2MB cap per topic, which
    gives adequate coverage for 4–8KB dictionaries.

    Individual samples (one per message) are stored, NOT concatenated,
    because zstd.train_dictionary() expects a list of individual samples.
    """

    def __init__(
        self,
        topic: str,
        max_samples: int = 10_000,
        max_bytes: int = 2 * 1024 * 1024,   # 2 MB
    ) -> None:
        self.topic = topic
        self.max_samples = max_samples
        self.max_bytes = max_bytes

        self._samples: list = []             # list[bytes], FIFO
        self._total_bytes: int = 0
        self._push_count: int = 0
        self._eviction_count: int = 0
        self._lock = threading.Lock()

    def push(self, sample: bytes) -> None:
        """Add one normalized message to the buffer. Evicts oldest if needed."""
        if not sample:
            return

        with self._lock:
            self._samples.append(sample)
            self._total_bytes += len(sample)
            self._push_count += 1

            # Evict from front until within limits
            while (len(self._samples) > self.max_samples
                   or self._total_bytes > self.max_bytes):
                evicted = self._samples.pop(0)
                self._total_bytes -= len(evicted)
                self._eviction_count += 1

    def get_samples(self) -> List[bytes]:
        """Return a snapshot of current samples (thread-safe copy)."""
        with self._lock:
            return list(self._samples)

    def ready_for_training(self, dict_size: int) -> bool:
        """
        Returns True if we have enough bytes to train a quality dictionary.
        Rule of thumb: ≥ 200× the target dict size in training data.
        """
        with self._lock:
            return self._total_bytes >= dict_size * 200

    def get_stats(self) -> dict:
        with self._lock:
            return {
                "topic": self.topic,
                "sample_count": len(self._samples),
                "total_bytes": self._total_bytes,
                "push_count": self._push_count,
                "eviction_count": self._eviction_count,
                "ready_ratio": round(self._total_bytes / (8192 * 200), 3),
            }


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _make_dict_id(topic: str, version: int, dict_bytes: bytes) -> str:
    """
    Deterministic, human-readable dictionary identifier.
    Format: d:{topic}:v{version}:{sha256[:12]}
    The hash makes it unique even if version counters reset.
    """
    sha = hashlib.sha256(dict_bytes).hexdigest()[:12]
    safe_topic = topic.replace(".", "_").replace("-", "_")
    return f"d:{safe_topic}:v{version}:{sha}"


def _build_version(
    topic: str,
    version: int,
    dict_bytes: bytes,
    compression_level: int,
) -> DictionaryVersion:
    """
    Construct a DictionaryVersion from raw dictionary bytes.
    Creates both compressor and decompressor from the same dict.
    """
    zd = zstd.ZstdCompressionDict(dict_bytes)
    compressor = zstd.ZstdCompressor(level=compression_level, dict_data=zd)
    decompressor = zstd.ZstdDecompressor(dict_data=zd)
    dict_id = _make_dict_id(topic, version, dict_bytes)
    return DictionaryVersion(
        dict_id=dict_id,
        topic=topic,
        version=version,
        dict_bytes=dict_bytes,
        compressor=compressor,
        decompressor=decompressor,
    )


# ──────────────────────────────────────────────────────────────────────────────
# DictionaryLifecycleManager
# ──────────────────────────────────────────────────────────────────────────────

class DictionaryLifecycleManager:
    """
    Orchestrates adaptive dictionary lifecycle across all topics.

    Core responsibilities:
      1. Load initial dictionaries from disk (compatible with v1 dict_index.json).
      2. Accept raw message samples for each topic (via record_sample).
      3. Accept batch metrics (via update_ratio) → feeds RatioMonitor.
      4. On drift_detected event → spawn background retrain thread.
      5. On retrain complete → blue/green swap: new dict_id for new batches,
         old DictionaryVersion stays alive for in-flight decompression.
      6. Periodically GC retired versions older than old_dict_ttl_seconds.
      7. Update dict_index.json on disk after each swap (for collector reload).

    Blue/Green rollout:
      - get_compressor(topic) → returns active version's {dict_id, compressor}
      - get_decompressor(dict_id) → searches ALL versions, including retired
      - Collector always finds a decompressor regardless of which version
        the edge agent used when compressing a batch.

    Thread safety:
      - Per-topic RatioMonitor and SampleBuffer have their own internal locks.
      - _versions, _active_idx, _retrain_in_progress share a single RLock
        on the manager level.

    Args:
        dict_dir:             Directory for reading/writing dict files and index.
        compression_level:    Zstd compression level for new compressors.
        dict_size:            Target dictionary size in bytes (default 8 KB).
        old_dict_ttl_seconds: How long to keep retired versions alive (default 5 min).
                              Must be > max expected in-flight batch latency.
        on_retrain_complete:  Optional callback(topic, new_version: DictionaryVersion)
                              called after each successful retrain.
        monitor_kwargs:       Extra kwargs forwarded to each RatioMonitor constructor.
    """

    def __init__(
        self,
        dict_dir: str,
        compression_level: int = 7,
        dict_size: int = 8192,
        old_dict_ttl_seconds: float = 300.0,
        on_retrain_complete: Optional[Callable[["DictionaryLifecycleManager", str, DictionaryVersion], None]] = None,
        monitor_kwargs: Optional[dict] = None,
    ) -> None:
        self.dict_dir = dict_dir
        self.compression_level = compression_level
        self.dict_size = dict_size
        self.old_dict_ttl_seconds = old_dict_ttl_seconds
        self.on_retrain_complete = on_retrain_complete
        self._monitor_kwargs = monitor_kwargs or {}

        # per-topic state
        self._versions: Dict[str, List[DictionaryVersion]] = {}   # topic → [v0, v1, ...]
        self._active_idx: Dict[str, int] = {}                      # topic → index into _versions[topic]
        self._monitors: Dict[str, RatioMonitor] = {}
        self._sample_buffers: Dict[str, SampleBuffer] = {}
        self._retrain_in_progress: Set[str] = set()

        self._lock = threading.RLock()

        os.makedirs(dict_dir, exist_ok=True)
        _info("dict_lifecycle_manager_initialized",
              dict_dir=dict_dir,
              dict_size=dict_size,
              compression_level=compression_level)

    # ── Loading initial dictionaries (v1 compatibility) ───────────────────────

    def load_from_index(self, index_path: Optional[str] = None) -> int:
        """
        Load all dictionaries listed in dict_index.json.
        Compatible with v1 format: {topic: {dict_id, path}}.
        Returns number of topics loaded.
        """
        if index_path is None:
            index_path = os.path.join(self.dict_dir, "dict_index.json")

        if not os.path.exists(index_path):
            _warn("dict_index_not_found", path=index_path)
            return 0

        try:
            with open(index_path, "r") as f:
                index: dict = json.load(f)
        except Exception as e:
            _error("failed_to_load_dict_index", path=index_path, error=str(e))
            return 0

        loaded = 0
        for topic, meta in index.items():
            path = meta.get("path", "")
            if not os.path.exists(path):
                _warn("dict_file_missing", topic=topic, path=path)
                continue
            try:
                with open(path, "rb") as f:
                    dict_bytes = f.read()
                self.register_dict(topic, dict_bytes, version=0)
                loaded += 1
            except Exception as e:
                _error("failed_to_load_dict", topic=topic, path=path, error=str(e))

        _info("dicts_loaded_from_index", count=loaded, path=index_path)
        return loaded

    def register_dict(self, topic: str, dict_bytes: bytes, version: int = 0) -> DictionaryVersion:
        """
        Register a dictionary for a topic (initial load or external injection).
        If a version already exists for this topic, this becomes the active one.
        """
        dv = _build_version(topic, version, dict_bytes, self.compression_level)
        with self._lock:
            if topic not in self._versions:
                self._versions[topic] = []
            self._versions[topic].append(dv)
            self._active_idx[topic] = len(self._versions[topic]) - 1
            self._ensure_monitor(topic)
            self._ensure_sample_buffer(topic)
        _info("dict_registered",
              topic=topic,
              dict_id=dv.dict_id,
              version=version,
              size=dv.size_bytes)
        return dv

    # ── Hot path: sample ingestion and ratio tracking ─────────────────────────

    def record_sample(self, topic: str, raw_message: bytes) -> None:
        """
        Feed one normalized (pre-compression) message into the sample buffer.
        Call this in enqueue() before the message enters the batch deque.
        Very low overhead: a single list.append under a lock.
        """
        buf = self._get_or_create_buffer(topic)
        buf.push(raw_message)

    def update_ratio(
        self,
        topic: str,
        raw_bytes: int,
        comp_bytes: int,
    ) -> RatioEvent:
        """
        Update the ratio monitor for this topic after each flush.
        Triggers background retrain if drift is detected.
        Returns the RatioEvent for the caller to log/inspect.
        """
        monitor = self._get_or_create_monitor(topic)
        event = monitor.update(raw_bytes, comp_bytes)

        if event.event_type == "drift_detected":
            self._maybe_trigger_retrain(topic)

        return event

    # ── Query: get compressor / decompressor ──────────────────────────────────

    def get_compressor(self, topic: str) -> Optional[Dict]:
        """
        Return {dict_id, compressor} for the currently active version.
        Returns None if no dictionary registered for this topic.
        Replaces the old self.compressors[topic] lookup in edge_agent.
        """
        with self._lock:
            if topic not in self._versions or not self._versions[topic]:
                return None
            idx = self._active_idx.get(topic, 0)
            dv = self._versions[topic][idx]
            return {"dict_id": dv.dict_id, "compressor": dv.compressor}

    def get_decompressor(self, dict_id: str) -> Optional[zstd.ZstdDecompressor]:
        """
        Return a decompressor for ANY dict_id — including retired versions.
        This is the blue/green safety net: the collector always finds the
        right decompressor regardless of which version compressed a batch.
        O(topics × versions), but called only at decompression time (collector).
        """
        with self._lock:
            for versions in self._versions.values():
                for dv in versions:
                    if dv.dict_id == dict_id:
                        return dv.decompressor
        return None

    def get_active_dict_id(self, topic: str) -> Optional[str]:
        """Return the dict_id of the currently active version for a topic."""
        with self._lock:
            if topic not in self._versions:
                return None
            idx = self._active_idx.get(topic, 0)
            return self._versions[topic][idx].dict_id

    # ── Stats and diagnostics ─────────────────────────────────────────────────

    def get_stats(self) -> dict:
        """
        Full status snapshot: all topics, all versions, all monitors.
        Suitable for the /health endpoint or structured logging.
        """
        with self._lock:
            topics_stats = {}
            for topic, versions in self._versions.items():
                active_idx = self._active_idx.get(topic, 0)
                active = versions[active_idx] if versions else None
                topics_stats[topic] = {
                    "active_dict_id": active.dict_id if active else None,
                    "active_version": active.version if active else None,
                    "active_dict_size": active.size_bytes if active else 0,
                    "total_versions": len(versions),
                    "retired_versions": sum(1 for v in versions if not v.is_active),
                    "retrain_in_progress": topic in self._retrain_in_progress,
                    "monitor": self._monitors[topic].get_stats() if topic in self._monitors else None,
                    "sample_buffer": self._sample_buffers[topic].get_stats() if topic in self._sample_buffers else None,
                }
            return {
                "topics": topics_stats,
                "total_topics": len(self._versions),
                "total_versions": sum(len(v) for v in self._versions.values()),
            }

    # ── Maintenance ───────────────────────────────────────────────────────────

    def gc_retired_dicts(self) -> int:
        """
        Remove retired DictionaryVersion objects older than old_dict_ttl_seconds.
        Call periodically from a maintenance thread (e.g., every 60 seconds).
        Returns number of versions removed.
        """
        now = time.time()
        removed = 0
        with self._lock:
            for topic in list(self._versions.keys()):
                before = len(self._versions[topic])
                self._versions[topic] = [
                    dv for dv in self._versions[topic]
                    if dv.is_active or (now - dv.retired_at) < self.old_dict_ttl_seconds
                ]
                after = len(self._versions[topic])
                removed += before - after
                if before != after:
                    _debug("retired_dicts_gc",
                           topic=topic,
                           removed=before - after,
                           remaining=after)
        if removed:
            _info("dict_gc_complete", versions_removed=removed)
        return removed

    # ── Internal machinery ────────────────────────────────────────────────────

    def _maybe_trigger_retrain(self, topic: str) -> None:
        """Check prerequisites and spawn retrain thread if safe to do so."""
        with self._lock:
            if topic in self._retrain_in_progress:
                _debug("retrain_already_in_progress", topic=topic)
                return

            buf = self._sample_buffers.get(topic)
            if buf is None or not buf.ready_for_training(self.dict_size):
                stats = buf.get_stats() if buf else {}
                _warn("insufficient_samples_for_retrain",
                      topic=topic,
                      available_bytes=stats.get("total_bytes", 0),
                      required_bytes=self.dict_size * 200)
                return

            self._retrain_in_progress.add(topic)

        _info("retrain_triggered", topic=topic)
        t = threading.Thread(
            target=self._retrain_worker,
            args=(topic,),
            daemon=True,
            name=f"retrain-{topic}",
        )
        t.start()

    def _retrain_worker(self, topic: str) -> None:
        """
        Background thread: train a new dictionary from buffered samples,
        then perform the blue/green swap.

        Flow:
            1. Grab samples snapshot (non-blocking — just copies the list).
            2. Train new zstd dictionary (CPU-bound, may take 100–500ms).
            3. Persist new dict to disk.
            4. Update dict_index.json.
            5. Blue/green swap: new version becomes active, old is retired.
            6. Notify RatioMonitor to reset baseline.
            7. Fire on_retrain_complete callback.
        """
        try:
            t_start = time.time()

            # 1. Snapshot samples
            with self._lock:
                buf = self._sample_buffers.get(topic)
            samples = buf.get_samples() if buf else []

            if len(samples) < 10:
                _warn("too_few_samples_to_retrain", topic=topic, count=len(samples))
                return

            _info("retrain_started",
                  topic=topic,
                  sample_count=len(samples),
                  sample_bytes=sum(len(s) for s in samples))

            # 2. Train (CPU-bound — runs outside all locks)
            try:
                zdict_obj = zstd.train_dictionary(self.dict_size, samples)
                new_dict_bytes = zdict_obj.as_bytes()
            except Exception as e:
                _error("zstd_training_failed", topic=topic, error=str(e))
                return

            # 3. Determine new version number
            with self._lock:
                existing = self._versions.get(topic, [])
                new_version = max((v.version for v in existing), default=0) + 1

            # 4. Persist to disk
            safe_topic = topic.replace(".", "_").replace("/", "_")
            dict_filename = f"{safe_topic}.v{new_version}.dict"
            dict_path = os.path.join(self.dict_dir, dict_filename)
            try:
                with open(dict_path, "wb") as f:
                    f.write(new_dict_bytes)
            except Exception as e:
                _error("failed_to_write_dict", topic=topic, path=dict_path, error=str(e))
                return

            # 5. Build new DictionaryVersion object
            new_dv = _build_version(topic, new_version, new_dict_bytes, self.compression_level)

            # 6. Blue/green swap (under lock)
            with self._lock:
                if topic not in self._versions:
                    self._versions[topic] = []

                # Retire current active version
                active_idx = self._active_idx.get(topic)
                if active_idx is not None and active_idx < len(self._versions[topic]):
                    old_dv = self._versions[topic][active_idx]
                    old_dv.retired_at = time.time()
                    _info("dict_version_retired",
                          topic=topic,
                          dict_id=old_dv.dict_id,
                          version=old_dv.version)

                # Activate new version
                self._versions[topic].append(new_dv)
                self._active_idx[topic] = len(self._versions[topic]) - 1

            # 7. Update dict_index.json on disk
            self._update_index_on_disk(topic, new_dv, dict_path)

            # 8. Reset ratio monitor baseline to new dict's performance
            with self._lock:
                monitor = self._monitors.get(topic)
            if monitor:
                monitor.notify_retrained()

            elapsed = (time.time() - t_start) * 1000
            _info("retrain_complete",
                  topic=topic,
                  new_dict_id=new_dv.dict_id,
                  new_version=new_version,
                  dict_size=new_dv.size_bytes,
                  elapsed_ms=round(elapsed, 1))

            # 9. Fire callback (e.g., to signal collector to reload)
            if self.on_retrain_complete:
                try:
                    self.on_retrain_complete(self, topic, new_dv)
                except Exception as e:
                    _error("on_retrain_complete_callback_failed", topic=topic, error=str(e))

        except Exception as e:
            _error("retrain_worker_uncaught", topic=topic, error=str(e))
        finally:
            with self._lock:
                self._retrain_in_progress.discard(topic)

    def _update_index_on_disk(
        self,
        topic: str,
        dv: DictionaryVersion,
        dict_path: str,
    ) -> None:
        """
        Atomically update dict_index.json to include the new version.
        Uses write-to-temp-then-rename for crash safety.
        """
        index_path = os.path.join(self.dict_dir, "dict_index.json")
        tmp_path = index_path + ".tmp"
        try:
            # Load existing index
            index: dict = {}
            if os.path.exists(index_path):
                with open(index_path, "r") as f:
                    index = json.load(f)

            # Update entry for this topic
            index[topic] = {
                "dict_id": dv.dict_id,
                "path": dict_path,
                "version": dv.version,
                "created_at": dv.created_at,
            }

            # Write atomically
            with open(tmp_path, "w") as f:
                json.dump(index, f, indent=2)
            os.replace(tmp_path, index_path)

            _debug("dict_index_updated", topic=topic, dict_id=dv.dict_id)
        except Exception as e:
            _error("failed_to_update_dict_index", topic=topic, error=str(e))
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

    def _get_or_create_monitor(self, topic: str) -> RatioMonitor:
        with self._lock:
            return self._ensure_monitor(topic)

    def _get_or_create_buffer(self, topic: str) -> SampleBuffer:
        with self._lock:
            return self._ensure_sample_buffer(topic)

    def _ensure_monitor(self, topic: str) -> RatioMonitor:
        """Must be called under self._lock."""
        if topic not in self._monitors:
            self._monitors[topic] = RatioMonitor(topic, **self._monitor_kwargs)
        return self._monitors[topic]

    def _ensure_sample_buffer(self, topic: str) -> SampleBuffer:
        """Must be called under self._lock."""
        if topic not in self._sample_buffers:
            self._sample_buffers[topic] = SampleBuffer(topic)
        return self._sample_buffers[topic]

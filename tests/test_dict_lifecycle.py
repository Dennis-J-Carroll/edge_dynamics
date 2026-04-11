# SPDX-License-Identifier: Apache-2.0
"""
tests/edge_utils/test_dict_lifecycle.py

Tests for RatioMonitor, SampleBuffer, and DictionaryLifecycleManager.

Structure:
    TestRatioMonitor           — EMA math, warmup, state machine
    TestSampleBuffer           — push, eviction, training readiness
    TestDictionaryLifecycle    — load, swap, retrain, GC
    TestBlueGreenHandoff       — old decompressors remain alive post-swap
    TestIntegration            — end-to-end: drift → retrain → new dict active
"""

import json
import os
import threading
import time

import pytest
import zstandard as zstd

from edge_dynamics.edge_utils.dict_lifecycle import (
    DictionaryLifecycleManager,
    DictionaryVersion,
    RatioEvent,
    RatioMonitor,
    SampleBuffer,
    _make_dict_id,
    _build_version,
)


# ──────────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def tmp_dict_dir(tmp_path):
    """Temporary directory for dictionary files."""
    d = tmp_path / "dicts"
    d.mkdir()
    return str(d)


@pytest.fixture
def sample_dict_bytes():
    """
    A real, trained zstd dictionary from synthetic IoT-like samples.
    Small (1 KB target) so tests stay fast.
    """
    samples = []
    for i in range(500):
        sample = json.dumps({
            "sensor_id": f"s{i % 10:03d}",
            "type": "temperature",
            "value": 22.0 + (i % 5) * 0.1,
            "unit": "celsius",
            "location": "room-a",
        }).encode()
        samples.append(sample)
    zd = zstd.train_dictionary(1024, samples)
    return zd.as_bytes()


@pytest.fixture
def dlm(tmp_dict_dir):
    """DictionaryLifecycleManager with a temp dict dir."""
    return DictionaryLifecycleManager(
        dict_dir=tmp_dict_dir,
        compression_level=3,   # fast for tests
        dict_size=1024,        # small for tests
        old_dict_ttl_seconds=2.0,  # fast GC for tests
    )


# ──────────────────────────────────────────────────────────────────────────────
# TestRatioMonitor
# ──────────────────────────────────────────────────────────────────────────────

class TestRatioMonitor:
    """Tests for EMA math, warmup, and state machine."""

    def test_initial_state_is_warmup(self):
        m = RatioMonitor("sensors.temp")
        assert m._state == "warmup"
        assert m._ema is None
        assert m._baseline is None

    def test_ema_seeds_on_first_update(self):
        m = RatioMonitor("sensors.temp")
        event = m.update(raw_bytes=1000, comp_bytes=100)
        assert m._ema == pytest.approx(0.10)
        assert event.event_type == "ratio_update"  # still in warmup

    def test_ema_convergence(self):
        """
        EMA with alpha=0.10 on constant input 0.05 should converge to 0.05.
        After 50 updates it should be within 0.001 of the true value.
        """
        m = RatioMonitor("sensors.temp", alpha=0.10, warmup_batches=5)
        for _ in range(50):
            m.update(raw_bytes=1000, comp_bytes=50)
        assert m._ema == pytest.approx(0.05, abs=0.001)

    def test_ema_update_is_difference_equation(self):
        """
        Verify: ema_t = ema_{t-1} + α*(ratio_t - ema_{t-1})
        This is the Minus One Principle in action: correction = difference.
        """
        m = RatioMonitor("t", alpha=0.2, warmup_batches=1)
        # seed EMA
        m.update(raw_bytes=1000, comp_bytes=100)  # ratio=0.10, ema=0.10
        old_ema = m._ema

        # manually compute expected
        ratio = 0.05
        expected_ema = old_ema + 0.2 * (ratio - old_ema)
        m.update(raw_bytes=1000, comp_bytes=50)
        assert m._ema == pytest.approx(expected_ema, rel=1e-9)

    def test_warmup_complete_event_fires_at_correct_batch(self):
        m = RatioMonitor("t", warmup_batches=5)
        events = [m.update(1000, 60).event_type for _ in range(6)]
        # First 4 updates: still in warmup → "ratio_update"
        # 5th update: transitions → "warmup_complete"
        # 6th update: in normal state → "ratio_update"
        assert events[4] == "warmup_complete"
        assert events[5] == "ratio_update"
        assert m._baseline is not None

    def test_drift_detection_requires_consecutive_batches(self):
        """
        A single anomalous batch should NOT trigger drift_detected.
        consecutive_trigger=3 means 3 in a row where _is_drifted() returns True.

        Uses alpha=0.5 (reactive) so the EMA crosses the drift threshold
        within the first bad batch — isolating the consecutive_trigger logic.
        """
        m = RatioMonitor(
            "t",
            alpha=0.5,              # reactive — EMA crosses threshold immediately
            warmup_batches=5,
            drift_threshold=0.10,   # 10% relative
            consecutive_trigger=3,
        )

        # Complete warmup at stable low ratio
        for _ in range(5):
            m.update(1000, 56)  # ratio = 0.056

        baseline = m._baseline

        # Inject ratio 2× above baseline → _is_drifted() = True from batch 1
        # But consecutive_trigger=3 means state stays "normal" until 3rd bad batch
        event1 = m.update(1000, int(baseline * 2.0 * 1000))
        assert event1.event_type != "drift_detected", "1 bad batch should NOT trigger"
        assert m._state == "normal"

        event2 = m.update(1000, int(baseline * 2.0 * 1000))
        assert event2.event_type != "drift_detected", "2 bad batches should NOT trigger"
        assert m._state == "normal"

        # Third consecutive drifted batch → fires drift_detected
        event3 = m.update(1000, int(baseline * 2.0 * 1000))
        assert event3.event_type == "drift_detected", "3rd consecutive should trigger"
        assert m._state == "drifted"

    def test_recovery_transitions_back_to_normal(self):
        """
        After drift state, returning to baseline ratio should emit 'recovered'
        and transition back to 'normal'.
        Uses alpha=0.5 so EMA is reactive enough to recover in ~5 batches.
        """
        m = RatioMonitor(
            "t",
            alpha=0.5,
            warmup_batches=5,
            drift_threshold=0.10,
            consecutive_trigger=1,   # immediate drift for test simplicity
            recovery_threshold=0.05,
        )

        for _ in range(5):
            m.update(1000, 56)  # ratio = 0.056, establish baseline

        baseline = m._baseline

        # Force into drifted state
        for _ in range(3):
            m.update(1000, int(baseline * 2.0 * 1000))

        assert m._state == "drifted"

        # Feed batches at original low ratio → EMA converges back
        recovered_event = None
        for _ in range(10):
            ev = m.update(1000, 56)
            if ev.event_type == "recovered":
                recovered_event = ev
                break

        assert recovered_event is not None, "Should emit 'recovered' as EMA returns to baseline"
        assert m._state == "normal"

    def test_notify_retrained_resets_baseline_to_current_ema(self):
        m = RatioMonitor("t", warmup_batches=3)
        for _ in range(5):
            m.update(1000, 50)

        ema_before = m._ema
        m.notify_retrained()
        assert m._baseline == pytest.approx(ema_before)
        assert m._state == "normal"
        assert m._retrain_count == 1

    def test_get_stats_structure(self):
        m = RatioMonitor("t")
        m.update(1000, 60)
        stats = m.get_stats()
        assert "ema" in stats
        assert "baseline" in stats
        assert "state" in stats
        assert "batch_count" in stats
        assert stats["batch_count"] == 1

    def test_thread_safety_concurrent_updates(self):
        """
        100 threads each calling update() 100 times.
        batch_count must equal 10000 exactly — no lost updates.
        """
        m = RatioMonitor("t")
        barrier = threading.Barrier(100)

        def worker():
            barrier.wait()
            for _ in range(100):
                m.update(1000, 56)

        threads = [threading.Thread(target=worker) for _ in range(100)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert m._batch_count == 10_000


# ──────────────────────────────────────────────────────────────────────────────
# TestSampleBuffer
# ──────────────────────────────────────────────────────────────────────────────

class TestSampleBuffer:
    """Tests for rolling sample window behavior."""

    def test_push_and_get_round_trip(self):
        buf = SampleBuffer("t", max_samples=100, max_bytes=10_000)
        buf.push(b"hello")
        buf.push(b"world")
        samples = buf.get_samples()
        assert samples == [b"hello", b"world"]

    def test_fifo_eviction_on_sample_count_limit(self):
        buf = SampleBuffer("t", max_samples=3, max_bytes=100_000)
        for i in range(5):
            buf.push(f"msg{i}".encode())
        samples = buf.get_samples()
        assert len(samples) == 3
        # Oldest evicted first
        assert b"msg2" in samples
        assert b"msg0" not in samples

    def test_fifo_eviction_on_byte_limit(self):
        buf = SampleBuffer("t", max_samples=10_000, max_bytes=20)
        buf.push(b"aaaaaaaaaa")   # 10 bytes
        buf.push(b"bbbbbbbbbb")   # 10 bytes → total 20, at limit
        buf.push(b"cccccccccc")   # 10 bytes → evict oldest
        samples = buf.get_samples()
        assert b"aaaaaaaaaa" not in samples
        assert b"cccccccccc" in samples

    def test_ready_for_training_threshold(self):
        """
        ready_for_training returns True only when ≥ 200× dict_size bytes.
        dict_size=1024 → need ≥ 204800 bytes.
        """
        buf = SampleBuffer("t", max_samples=100_000, max_bytes=10 * 1024 * 1024)
        dict_size = 1024
        sample = b"x" * 1000

        assert not buf.ready_for_training(dict_size)

        for _ in range(205):  # 205_000 bytes
            buf.push(sample)

        assert buf.ready_for_training(dict_size)

    def test_empty_push_ignored(self):
        buf = SampleBuffer("t")
        buf.push(b"")
        assert len(buf.get_samples()) == 0

    def test_thread_safety(self):
        buf = SampleBuffer("t", max_samples=10_000, max_bytes=1_000_000)
        barrier = threading.Barrier(50)

        def pusher():
            barrier.wait()
            for i in range(100):
                buf.push(f"sample_{i}".encode())

        threads = [threading.Thread(target=pusher) for _ in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All within limits
        assert len(buf.get_samples()) <= 10_000
        assert buf._total_bytes <= 1_000_000


# ──────────────────────────────────────────────────────────────────────────────
# TestDictionaryLifecycle
# ──────────────────────────────────────────────────────────────────────────────

class TestDictionaryLifecycle:
    """Tests for load, register, swap, and GC."""

    def test_load_from_index_valid(self, tmp_dict_dir, sample_dict_bytes):
        """load_from_index reads dict_index.json and registers all topics."""
        # Write a dict file and index
        dict_path = os.path.join(tmp_dict_dir, "sensors.temp.dict")
        with open(dict_path, "wb") as f:
            f.write(sample_dict_bytes)

        index = {"sensors.temp": {"dict_id": "d:sensors_temp:v0:abc", "path": dict_path}}
        index_path = os.path.join(tmp_dict_dir, "dict_index.json")
        with open(index_path, "w") as f:
            json.dump(index, f)

        manager = DictionaryLifecycleManager(tmp_dict_dir)
        loaded = manager.load_from_index()
        assert loaded == 1
        assert manager.get_compressor("sensors.temp") is not None

    def test_load_from_index_missing_file_is_skipped(self, tmp_dict_dir):
        index = {"ghost.topic": {"dict_id": "x", "path": "/nonexistent/path.dict"}}
        index_path = os.path.join(tmp_dict_dir, "dict_index.json")
        with open(index_path, "w") as f:
            json.dump(index, f)

        manager = DictionaryLifecycleManager(tmp_dict_dir)
        loaded = manager.load_from_index()
        assert loaded == 0

    def test_register_dict_returns_version_object(self, dlm, sample_dict_bytes):
        dv = dlm.register_dict("t1", sample_dict_bytes, version=0)
        assert isinstance(dv, DictionaryVersion)
        assert dv.topic == "t1"
        assert dv.version == 0
        assert dv.is_active

    def test_get_compressor_returns_none_for_unknown_topic(self, dlm):
        assert dlm.get_compressor("unknown.topic") is None

    def test_get_compressor_returns_active_compressor(self, dlm, sample_dict_bytes):
        dlm.register_dict("t1", sample_dict_bytes)
        comp_info = dlm.get_compressor("t1")
        assert comp_info is not None
        assert "dict_id" in comp_info
        assert "compressor" in comp_info
        assert isinstance(comp_info["compressor"], zstd.ZstdCompressor)

    def test_compressor_actually_compresses(self, dlm, sample_dict_bytes):
        """The compressor from get_compressor should actually compress data."""
        dlm.register_dict("t1", sample_dict_bytes)
        comp_info = dlm.get_compressor("t1")
        raw = b'{"sensor_id":"s001","type":"temperature","value":22.1}' * 10
        compressed = comp_info["compressor"].compress(raw)
        assert len(compressed) < len(raw)

    def test_get_decompressor_finds_active_version(self, dlm, sample_dict_bytes):
        dv = dlm.register_dict("t1", sample_dict_bytes)
        dec = dlm.get_decompressor(dv.dict_id)
        assert dec is not None
        assert isinstance(dec, zstd.ZstdDecompressor)

    def test_get_decompressor_returns_none_for_unknown_id(self, dlm):
        assert dlm.get_decompressor("d:nonexistent:v0:xxxxxxxx") is None

    def test_gc_removes_old_retired_versions(self, tmp_dict_dir, sample_dict_bytes):
        manager = DictionaryLifecycleManager(
            tmp_dict_dir, old_dict_ttl_seconds=0.01
        )
        # Manually build and retire a version
        dv0 = manager.register_dict("t1", sample_dict_bytes, version=0)

        # Retire it manually
        with manager._lock:
            manager._versions["t1"][0].retired_at = time.time() - 1.0  # 1s ago

        time.sleep(0.05)
        removed = manager.gc_retired_dicts()
        assert removed == 1

    def test_gc_keeps_active_version(self, dlm, sample_dict_bytes):
        dlm.register_dict("t1", sample_dict_bytes)
        removed = dlm.gc_retired_dicts()
        assert removed == 0
        assert dlm.get_compressor("t1") is not None


# ──────────────────────────────────────────────────────────────────────────────
# TestBlueGreenHandoff
# ──────────────────────────────────────────────────────────────────────────────

class TestBlueGreenHandoff:
    """
    After a retrain and swap, the OLD decompressor must still be retrievable
    by dict_id. This is the safety net that prevents decompression failures
    on in-flight batches that were compressed before the swap.
    """

    def test_old_decompressor_survives_blue_green_swap(self, dlm, sample_dict_bytes):
        # Register initial dict (version 0)
        dv0 = dlm.register_dict("t1", sample_dict_bytes, version=0)
        old_dict_id = dv0.dict_id

        # Simulate a swap: register a second dict as version 1
        # (In production, _retrain_worker does this; we test directly.)
        new_dict_bytes = sample_dict_bytes  # same bytes, but new version for test
        dv1 = _build_version("t1", 1, new_dict_bytes, compression_level=3)

        with dlm._lock:
            # Retire dv0
            dlm._versions["t1"][0].retired_at = time.time()
            # Activate dv1
            dlm._versions["t1"].append(dv1)
            dlm._active_idx["t1"] = 1

        # New compressor is v1
        comp_info = dlm.get_compressor("t1")
        assert comp_info["dict_id"] == dv1.dict_id

        # But OLD decompressor is still findable by its dict_id
        old_dec = dlm.get_decompressor(old_dict_id)
        assert old_dec is not None, "Old decompressor must survive blue/green swap"

    def test_compress_decompress_roundtrip_across_swap(self, dlm, sample_dict_bytes):
        """
        Compress data with v0, swap to v1, decompress with v0 decompressor.
        Data must be recovered exactly.
        """
        dv0 = dlm.register_dict("t1", sample_dict_bytes, version=0)
        raw = b'{"sensor_id":"s001","value":22.3,"unit":"celsius"}' * 20

        # Compress with v0
        comp_info_v0 = dlm.get_compressor("t1")
        compressed = comp_info_v0["compressor"].compress(raw)
        old_dict_id = comp_info_v0["dict_id"]

        # Swap to v1 (same bytes, different version number for test)
        dv1 = _build_version("t1", 1, sample_dict_bytes, compression_level=3)
        with dlm._lock:
            dlm._versions["t1"][0].retired_at = time.time()
            dlm._versions["t1"].append(dv1)
            dlm._active_idx["t1"] = 1

        # Decompress using old dict_id
        old_dec = dlm.get_decompressor(old_dict_id)
        assert old_dec is not None
        recovered = old_dec.decompress(compressed)
        assert recovered == raw


# ──────────────────────────────────────────────────────────────────────────────
# TestUpdateRatioIntegration
# ──────────────────────────────────────────────────────────────────────────────

class TestUpdateRatioIntegration:
    """Tests for the update_ratio → drift → retrain pipeline."""

    def test_update_ratio_returns_event(self, dlm, sample_dict_bytes):
        dlm.register_dict("t1", sample_dict_bytes)
        event = dlm.update_ratio("t1", raw_bytes=1000, comp_bytes=56)
        assert isinstance(event, RatioEvent)
        assert event.topic == "t1"

    def test_update_ratio_creates_monitor_for_new_topic(self, dlm):
        """update_ratio works even without a prior register_dict call."""
        event = dlm.update_ratio("new.topic", 1000, 56)
        assert event is not None
        with dlm._lock:
            assert "new.topic" in dlm._monitors

    def test_record_sample_populates_buffer(self, dlm):
        sample = b'{"sensor_id":"s001","value":22.0}'
        dlm.record_sample("t1", sample)
        with dlm._lock:
            buf = dlm._sample_buffers.get("t1")
        assert buf is not None
        assert len(buf.get_samples()) == 1

    def test_drift_triggers_retrain_with_sufficient_samples(
        self, tmp_dict_dir, sample_dict_bytes
    ):
        """
        End-to-end: pump enough samples + drift signal → retrain fires.
        Uses a callback to detect completion without sleeping.
        """
        retrain_event = threading.Event()
        retrained_topics = []

        def on_retrain(manager, topic, new_version):
            retrained_topics.append(topic)
            retrain_event.set()

        manager = DictionaryLifecycleManager(
            tmp_dict_dir,
            compression_level=3,
            dict_size=1024,
            on_retrain_complete=on_retrain,
            monitor_kwargs={
                "warmup_batches": 5,
                "drift_threshold": 0.10,
                "consecutive_trigger": 1,
                "alpha": 0.5,  # highly reactive for test
            },
        )
        manager.register_dict("sensors.temp", sample_dict_bytes)

        # Fill sample buffer beyond training threshold (1024 * 200 = ~200KB)
        sample = b'{"sensor_id":"s001","type":"temperature","value":22.0}' * 5
        for _ in range(800):
            manager.record_sample("sensors.temp", sample)

        # Warmup phase — feed normal ratios
        for _ in range(5):
            manager.update_ratio("sensors.temp", 1000, 60)  # ratio ≈ 0.06

        # Now inject severe degradation → triggers drift
        for _ in range(3):
            manager.update_ratio("sensors.temp", 1000, 200)  # ratio = 0.20 (+233% of baseline)

        # Wait for background retrain (up to 10 seconds)
        fired = retrain_event.wait(timeout=10.0)
        assert fired, "Retrain did not complete within timeout"
        assert "sensors.temp" in retrained_topics

        # New version should be active
        comp_info = manager.get_compressor("sensors.temp")
        assert comp_info is not None
        assert "v1" in comp_info["dict_id"]  # version number incremented

    def test_retrain_updates_dict_index_on_disk(self, tmp_dict_dir, sample_dict_bytes):
        """After retrain, dict_index.json on disk must reflect the new version."""
        done = threading.Event()

        def on_retrain(manager, topic, new_version):
            done.set()

        manager = DictionaryLifecycleManager(
            tmp_dict_dir,
            compression_level=3,
            dict_size=1024,
            on_retrain_complete=on_retrain,
            monitor_kwargs={
                "warmup_batches": 5,
                "drift_threshold": 0.10,
                "consecutive_trigger": 1,
                "alpha": 0.5,
            },
        )
        manager.register_dict("sensors.temp", sample_dict_bytes)

        sample = b'{"sensor_id":"s001","value":22.0}' * 8
        for _ in range(900):
            manager.record_sample("sensors.temp", sample)

        for _ in range(5):
            manager.update_ratio("sensors.temp", 1000, 60)

        for _ in range(3):
            manager.update_ratio("sensors.temp", 1000, 300)

        fired = done.wait(timeout=10.0)
        assert fired

        # Check disk
        index_path = os.path.join(tmp_dict_dir, "dict_index.json")
        assert os.path.exists(index_path)
        with open(index_path) as f:
            index = json.load(f)
        assert "sensors.temp" in index
        assert "v1" in index["sensors.temp"]["dict_id"]


# ──────────────────────────────────────────────────────────────────────────────
# TestHelpers
# ──────────────────────────────────────────────────────────────────────────────

class TestHelpers:
    def test_make_dict_id_format(self, sample_dict_bytes):
        did = _make_dict_id("sensors.temp", 3, sample_dict_bytes)
        assert did.startswith("d:sensors_temp:v3:")
        assert len(did) > 20

    def test_make_dict_id_deterministic(self, sample_dict_bytes):
        d1 = _make_dict_id("t", 1, sample_dict_bytes)
        d2 = _make_dict_id("t", 1, sample_dict_bytes)
        assert d1 == d2

    def test_make_dict_id_changes_with_different_bytes(self, sample_dict_bytes):
        d1 = _make_dict_id("t", 1, sample_dict_bytes)
        d2 = _make_dict_id("t", 1, b"different_content")
        assert d1 != d2

    def test_build_version_creates_working_compressor(self, sample_dict_bytes):
        dv = _build_version("t", 0, sample_dict_bytes, compression_level=3)
        raw = b"test data " * 100
        compressed = dv.compressor.compress(raw)
        recovered = dv.decompressor.decompress(compressed)
        assert recovered == raw

    def test_dictionary_version_is_active_property(self, sample_dict_bytes):
        dv = _build_version("t", 0, sample_dict_bytes, compression_level=3)
        assert dv.is_active
        dv.retired_at = time.time()
        assert not dv.is_active

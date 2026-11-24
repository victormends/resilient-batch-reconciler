"""Tests for incremental windows, heap ETA simulator, checkpoints, and spec-hash cache."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from reconciler.orchestrator import (
    StageCheckpoint,
    TemplateHashCache,
    calibrate_median_durations,
    compute_incremental_window,
    estimate_fleet_completion,
)


def test_compute_incremental_window_with_overlap_and_clamping() -> None:
    now = datetime(2026, 11, 24, 12, 0, 0, tzinfo=timezone.utc)

    # No checkpoint -> default_seconds
    w_none = compute_incremental_window(None, now=now, default_seconds=43_200)
    assert w_none.window_seconds == 43_200
    assert w_none.end_at == now
    assert w_none.start_at == now - timedelta(seconds=43_200)

    # 3 hours elapsed + 2h overlap buffer = 5h = 18000s
    last = now - timedelta(hours=3)
    w_ok = compute_incremental_window(last, now=now, overlap_buffer_seconds=7_200)
    assert w_ok.window_seconds == 18_000
    assert w_ok.clamped is False

    # Very old checkpoint clamped to max_seconds
    very_old = now - timedelta(days=30)
    w_max = compute_incremental_window(very_old, now=now, max_seconds=604_800)
    assert w_max.window_seconds == 604_800
    assert w_max.clamped is True

    # Future timestamp clamped safely to min_seconds
    future = now + timedelta(minutes=10)
    w_future = compute_incremental_window(future, now=now, min_seconds=60)
    assert w_future.window_seconds == 60
    assert w_future.clamped is True


def test_calibrate_medians_and_heap_fleet_eta_simulation() -> None:
    calibrated = calibrate_median_durations(
        {
            "cluster-a": [12.0, 10.0, 40.0, 14.0, 11.0],
            "cluster-b": [-5.0, 0.0],
        },
        fallback_durations={"cluster-a": 100.0, "cluster-b": 25.0},
    )
    assert calibrated["cluster-a"] == pytest.approx(12.0)
    assert calibrated["cluster-b"] == pytest.approx(25.0)

    # 4 queued tasks [10, 20, 30, 40], concurrency=2, stagger=5s at t=0:
    # Slot 0 starts task-1 at 0 -> free at 10 -> picks task-3 (30) -> done at 40
    # Slot 1 starts task-2 at 5 -> free at 25 -> picks task-4 (40) -> done at 65
    eta = estimate_fleet_completion(
        ["t1", "t2", "t3", "t4"],
        {"t1": 10.0, "t2": 20.0, "t3": 30.0, "t4": 40.0},
        concurrency=2,
        stagger_sec=5.0,
        now_ts=1000.0,
        started_at_ts=1000.0,
    )
    assert eta.estimated_remaining_seconds == pytest.approx(65.0)
    assert eta.worker_slot_loads == [40.0, 65.0]
    assert eta.queued_count == 4

    # Active in-flight + completed task simulation
    eta_mid = estimate_fleet_completion(
        ["t1", "t2", "t3", "t4"],
        {"t1": 10.0, "t2": 20.0, "t3": 30.0, "t4": 40.0},
        concurrency=2,
        completed_tasks=["t1"],
        active_started_at={"t2": 990.0},  # t2 ran 10s of 20s -> 10s left
        now_ts=1000.0,
        started_at_ts=985.0,
    )
    # Slot A (t2 active): 10s left; Slot B (t3 queued): 30s -> Slot A pops at 10s + t4(40s) = 50s
    assert eta_mid.estimated_remaining_seconds == pytest.approx(50.0)
    assert eta_mid.completed_count == 1
    assert eta_mid.active_count == 1
    assert eta_mid.queued_count == 2


def test_stage_checkpoint_idempotent_resumption(tmp_path: Path) -> None:
    ckpt_path = tmp_path / "checkpoint.json"
    ckpt = StageCheckpoint(ckpt_path)

    all_nodes = ["n1", "n2", "n3", "n4"]
    ckpt.mark_item_done("probe_fusion", "n1", metadata={"ok": True})
    ckpt.mark_items_done("probe_fusion", ["n3", "n1"])  # n1 idempotent

    # Simulate restart by instantiating a fresh StageCheckpoint
    resumed = StageCheckpoint(ckpt_path)
    assert resumed.filter_pending("probe_fusion", all_nodes) == ["n2", "n4"]
    assert resumed.is_stage_completed("probe_fusion") is False

    resumed.mark_stage_completed("probe_fusion")
    run_ts = resumed.save_last_successful_run(
        completed_at=datetime(2026, 11, 24, 10, 0, 0, tzinfo=timezone.utc)
    )
    assert resumed.is_stage_completed("probe_fusion") is True
    assert resumed.last_successful_run_at() == run_ts

    window = compute_incremental_window(
        resumed.last_successful_run_at(),
        now=datetime(2026, 11, 24, 11, 0, 0, tzinfo=timezone.utc),
        overlap_buffer_seconds=1800,
    )
    assert window.window_seconds == 5400


def test_template_hash_cache_ttl_and_sha256_invalidation(tmp_path: Path) -> None:
    now_holder = {"ts": 1000.0}
    cache_path = tmp_path / "template_cache.json"
    cache = TemplateHashCache(cache_path, clock_ts=lambda: now_holder["ts"])

    spec_v1 = {"prompt": "Classify SRE dashboard grid", "schema_v": 1}
    spec_v2 = {"prompt": "Classify SRE dashboard grid", "schema_v": 2}

    cache.set("grid-001", {"anomaly": False}, spec=spec_v1, ttl_seconds=120.0)
    assert cache.get("grid-001", spec=spec_v1) == {"anomaly": False}

    # Changing spec immediately invalidates via sha256 mismatch
    assert cache.get("grid-001", spec=spec_v2) is None

    # Advancing clock past TTL invalidates even for matching spec_v1
    now_holder["ts"] = 1121.0
    assert cache.get("grid-001", spec=spec_v1) is None

    # Test cleanup helpers (advance clock monotonically forward)
    now_holder["ts"] = 1200.0
    assert cache.prune_expired() == 1  # grid-001 expired at 1120.0
    cache.set("keep-v2", {"ok": True}, spec=spec_v2, ttl_seconds=600.0)
    cache.set("old-v1", {"ok": True}, spec=spec_v1, ttl_seconds=10.0)
    now_holder["ts"] = 1220.0
    assert cache.prune_expired() == 1
    assert cache.get("keep-v2", spec=spec_v2) == {"ok": True}

    cache.set("another-v1", {"ok": True}, spec=spec_v1, ttl_seconds=600.0)
    assert cache.invalidate_mismatched_specs(spec_v2) == 1
    assert cache.get("keep-v2", spec=spec_v2) == {"ok": True}

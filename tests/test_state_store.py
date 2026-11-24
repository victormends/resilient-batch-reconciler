"""Tests for AtomicJsonStore concurrency, Windows retry resilience, and monotonic asset ledger."""

from __future__ import annotations

import multiprocessing
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Tuple

import pytest

from reconciler.state_store import (
    AtomicJsonStore,
    StoreCorruptionError,
    reconcile_asset_record,
)


def _mp_worker_increment(args: Tuple[str, str, int, int]) -> None:
    """Top-level multiprocessing worker (spawn-safe on Windows)."""
    src_dir, store_path, worker_id, iterations = args
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)
    from reconciler.state_store import AtomicJsonStore as ProcStore

    store = ProcStore(store_path, lock_timeout=30.0)
    for idx in range(iterations):
        store.update(
            lambda doc: {
                **doc,
                "total_ops": int(doc.get("total_ops", 0)) + 1,
                "worker_keys": {
                    **(doc.get("worker_keys") or {}),
                    f"w{worker_id}_i{idx}": True,
                },
            }
        )
        store.reconcile_asset(
            "shared-cluster-node-01",
            {
                "region": "us-east-1" if idx % 2 == 0 else None,
                "cpu_cores": 32,
                "probes_seen": [f"proc-{worker_id}"],
            },
            source=f"worker-{worker_id}",
            observed_at=f"2026-11-24T00:00:{worker_id:02d}Z",
        )


def test_monotonic_reconciliation_never_overwrites_confirmed_facts() -> None:
    initial = reconcile_asset_record(
        None,
        {
            "asset_id": "prod-us-east-1/api-01",
            "region": "us-east-1",
            "owner_team": "checkout-sre",
            "cpu_cores": 16,
            "error_rate": 0.0,
            "deprecated": False,
            "tags": ["tier-1", "pci"],
            "labels": {"env": "prod", "zone": "az-1a"},
        },
        source="aws-inventory",
        observed_at="2026-11-24T01:00:00Z",
    )

    assert initial["region"] == "us-east-1"
    assert initial["error_rate"] == 0.0
    assert initial["deprecated"] is False
    assert initial["observations_count"] == 1
    assert len(initial["evidence_ledger"]) == 1
    assert initial["evidence_ledger"][0]["source"] == "aws-inventory"

    degraded_probe = reconcile_asset_record(
        initial,
        {
            "asset_id": "prod-us-east-1/api-01",
            "region": None,
            "owner_team": "",
            "cpu_cores": None,
            "tags": [],
            "labels": {"env": "prod", "zone": None},
            "latency_ms": 14.2,
        },
        source="partial-ping-probe",
        observed_at="2026-11-24T01:05:00Z",
    )

    assert degraded_probe["region"] == "us-east-1"
    assert degraded_probe["owner_team"] == "checkout-sre"
    assert degraded_probe["cpu_cores"] == 16
    assert degraded_probe["error_rate"] == 0.0
    assert degraded_probe["deprecated"] is False
    assert degraded_probe["tags"] == ["tier-1", "pci"]
    assert degraded_probe["labels"] == {"env": "prod", "zone": "az-1a"}
    assert degraded_probe["latency_ms"] == 14.2
    assert degraded_probe["observations_count"] == 2
    assert set(degraded_probe["retained_fields"]) >= {
        "region",
        "owner_team",
        "cpu_cores",
        "error_rate",
        "deprecated",
        "tags",
        "labels.zone",
    }
    assert len(degraded_probe["evidence_ledger"]) == 2
    assert degraded_probe["evidence_ledger"][1]["source"] == "partial-ping-probe"
    assert degraded_probe["evidence_ledger"][1]["observed_at"] == "2026-11-24T01:05:00Z"


def test_atomic_store_retries_transient_windows_replace_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store_path = tmp_path / "inventory.json"
    sleeps = []
    store = AtomicJsonStore(
        store_path,
        max_retries=5,
        retry_delay=0.01,
        sleep_fn=lambda s: sleeps.append(s),
    )

    orig_replace = Path.replace
    attempts = {"count": 0}

    def flaky_replace(self: Path, target: Path) -> Path:
        attempts["count"] += 1
        if attempts["count"] <= 3:
            raise PermissionError("Simulated Windows indexer lock")
        return orig_replace(self, target)

    monkeypatch.setattr(Path, "replace", flaky_replace)

    saved = store.save({"asset_count": 42})
    assert saved == {"asset_count": 42}
    assert attempts["count"] == 4
    assert len(sleeps) == 3
    assert store.load() == {"asset_count": 42}
    assert list(tmp_path.glob("*.tmp")) == []


def test_atomic_store_concurrent_threads_zero_lost_updates(tmp_path: Path) -> None:
    store_path = tmp_path / "concurrent_threads.json"
    store = AtomicJsonStore(store_path)
    num_threads = 8
    writes_per_thread = 12

    def worker(tid: int) -> None:
        for seq in range(writes_per_thread):
            store.update(
                lambda doc: {
                    **doc,
                    "counter": int(doc.get("counter", 0)) + 1,
                }
            )
            store.reconcile_asset(
                f"node-{tid:02d}",
                {
                    "region": "eu-west-1" if seq == 0 else None,
                    "tags": [f"batch-{seq % 3}"],
                    "last_seq": seq,
                },
                source=f"thread-{tid}",
                observed_at=f"2026-11-24T02:{tid:02d}:{seq:02d}Z",
            )

    with ThreadPoolExecutor(max_workers=num_threads) as pool:
        futures = [pool.submit(worker, tid) for tid in range(num_threads)]
        for fut in futures:
            fut.result()

    final = store.load()
    assert final["counter"] == num_threads * writes_per_thread
    assert len(final["assets"]) == num_threads
    for tid in range(num_threads):
        rec = final["assets"][f"node-{tid:02d}"]
        assert rec["region"] == "eu-west-1"
        assert rec["observations_count"] == writes_per_thread
        assert len(rec["evidence_ledger"]) == writes_per_thread


def test_atomic_store_concurrent_processes_zero_lost_updates(tmp_path: Path) -> None:
    store_path = tmp_path / "concurrent_procs.json"
    src_dir = str(Path(__file__).resolve().parents[1] / "src")
    num_procs = 4
    ops_per_proc = 6

    ctx = multiprocessing.get_context("spawn")
    procs = [
        ctx.Process(
            target=_mp_worker_increment,
            args=((src_dir, str(store_path), pid, ops_per_proc),),
        )
        for pid in range(num_procs)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=30)
        assert p.exitcode == 0

    store = AtomicJsonStore(store_path)
    final = store.load()
    assert final["total_ops"] == num_procs * ops_per_proc
    assert len(final["worker_keys"]) == num_procs * ops_per_proc
    shared_asset = final["assets"]["shared-cluster-node-01"]
    assert shared_asset["region"] == "us-east-1"
    assert shared_asset["cpu_cores"] == 32
    assert shared_asset["observations_count"] == num_procs * ops_per_proc
    assert len(shared_asset["evidence_ledger"]) == num_procs * ops_per_proc
    assert len(shared_asset["probes_seen"]) == num_procs


def test_corrupt_json_quarantines_and_fails_closed(tmp_path: Path) -> None:
    corrupt_bytes = "{broken json payload!!!"
    store_path = tmp_path / "ledger.json"
    store_path.write_text(corrupt_bytes, encoding="utf-8")

    # Default policy='quarantine' creates a .corrupt.<ns> snapshot and fails closed.
    q_store = AtomicJsonStore(store_path)
    with pytest.raises(StoreCorruptionError):
        q_store.load()
    with pytest.raises(StoreCorruptionError):
        q_store.update(lambda d: {**d, "x": 1})

    quarantined = list(tmp_path.glob("ledger.json.corrupt.*"))
    assert len(quarantined) >= 1
    assert quarantined[0].read_text(encoding="utf-8") == corrupt_bytes
    assert store_path.read_text(encoding="utf-8") == corrupt_bytes

    # Explicit policy='raise' fails closed without creating new quarantine copies.
    before_count = len(list(tmp_path.glob("ledger.json.corrupt.*")))
    r_store = AtomicJsonStore(store_path, corrupt_policy="raise")
    with pytest.raises(StoreCorruptionError):
        r_store.load()
    assert len(list(tmp_path.glob("ledger.json.corrupt.*"))) == before_count

    # Explicit policy='reset' recovers cleanly only when opted in.
    reset_store = AtomicJsonStore(store_path, corrupt_policy="reset")
    assert reset_store.load() == {}


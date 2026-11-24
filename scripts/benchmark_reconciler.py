"""Local synthetic concurrency, quarantine, signal fusion, and deduplication benchmark."""

from __future__ import annotations

import json
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from reconciler import (
    AtomicJsonStore,
    HitlRemediationQueue,
    ProbeReading,
    RemediationItem,
    StoreCorruptionError,
    fuse_probe_readings,
)


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="reconciler-bench-") as tmp:
        root = Path(tmp)

        # 1. Multi-threaded AtomicJsonStore concurrent read-modify-write stress test
        threads_count = 8
        writes_per_thread = 25
        expected_writes = threads_count * writes_per_thread
        store_path = root / "concurrent_store.json"
        store = AtomicJsonStore(store_path)
        barrier = threading.Barrier(threads_count)
        errors: List[str] = []

        def _worker(tid: int) -> None:
            try:
                barrier.wait(timeout=10.0)
                for idx in range(writes_per_thread):
                    def _mutate(doc: Dict[str, Any], t: int = tid, i: int = idx) -> Dict[str, Any]:
                        doc["total_updates"] = int(doc.get("total_updates", 0)) + 1
                        entries = doc.setdefault("workers", {})
                        entries[f"w-{t}-{i}"] = True
                        return doc

                    store.update(_mutate)
            except Exception as exc:  # pragma: no cover - surfaced in summary
                errors.append(f"thread-{tid}: {exc}")

        t0 = time.perf_counter()
        workers = [threading.Thread(target=_worker, args=(tid,)) for tid in range(threads_count)]
        for w in workers:
            w.start()
        for w in workers:
            w.join()
        concurrent_elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)

        final_doc = store.load()
        observed_updates = int(final_doc.get("total_updates", 0))
        observed_keys = len(final_doc.get("workers", {}))
        assert not errors, f"Worker errors: {errors}"
        assert observed_updates == expected_writes, f"{observed_updates} != {expected_writes}"
        assert observed_keys == expected_writes, f"{observed_keys} != {expected_writes}"

        # 2. Corrupt JSON quarantine recovery verification
        corrupt_target = root / "corrupt_state.json"
        corrupt_payload = '{"assets": {"node-01": {"status": "degraded"'
        corrupt_target.write_text(corrupt_payload, encoding="utf-8")
        quarantine_store = AtomicJsonStore(corrupt_target, corrupt_policy="quarantine")

        quarantine_raised = False
        t0 = time.perf_counter()
        try:
            quarantine_store.load()
        except StoreCorruptionError:
            quarantine_raised = True
        quarantine_elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)
        quarantined_files = list(root.glob("corrupt_state.json.corrupt.*"))
        assert quarantine_raised
        assert len(quarantined_files) == 1
        assert quarantined_files[0].read_text(encoding="utf-8") == corrupt_payload

        # 3. 2-probe vs 3-probe inverse-variance conflict inflation
        t0 = time.perf_counter()
        two_probe_agreed = fuse_probe_readings(
            [
                ProbeReading("http_probe", 40.0, sigma=1.0, role="primary"),
                ProbeReading("metrics_exporter", 42.0, sigma=2.0, role="secondary"),
            ],
            conflict_threshold=15.0,
        )
        two_probe_conflict = fuse_probe_readings(
            [
                ProbeReading("http_probe", 40.0, sigma=1.0, role="primary"),
                ProbeReading("metrics_exporter", 95.0, sigma=2.0, role="secondary"),
            ],
            conflict_threshold=15.0,
        )
        three_probe_outlier = fuse_probe_readings(
            [
                ProbeReading("http_probe", 40.0, sigma=1.0, role="primary"),
                ProbeReading("metrics_exporter", 41.5, sigma=2.0, role="secondary"),
                ProbeReading("noisy_canary", 110.0, sigma=1.5, role="secondary"),
            ],
            conflict_threshold=15.0,
        )
        fusion_elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)
        assert two_probe_agreed is not None
        assert two_probe_conflict is not None
        assert three_probe_outlier is not None
        assert two_probe_conflict.conflict_flag is True
        assert two_probe_conflict.fused_sigma > two_probe_agreed.fused_sigma
        assert three_probe_outlier.effective_sigmas["noisy_canary"] > three_probe_outlier.effective_sigmas["http_probe"]

        # 4. HitlRemediationQueue deduplication across 50 items / 10 unique keys
        now = datetime(2026, 11, 24, 12, 0, 0, tzinfo=timezone.utc)
        queue = HitlRemediationQueue(root / "remediation_queue.json", clock=lambda: now)
        severities = ("low", "medium", "high", "critical")
        items = [
            RemediationItem(
                asset_id=f"prod-cluster/node-{i % 10:02d}",
                action="drain_and_restart",
                reason=f"synthetic alert wave #{i}",
                severity=severities[(i % 10) % len(severities)],
            )
            for i in range(50)
        ]
        t0 = time.perf_counter()
        enq = queue.enqueue_remediations(items, actor="bench-runner")
        leased = queue.drain_batch(limit=10, reviewer="oncall-sre")
        queue_elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)
        assert enq.added == 10
        assert enq.deduplicated == 40
        assert len(leased) == 10
        assert leased[0]["severity"] == "critical"

        report = {
            "harness": "local-synthetic-reconciler-benchmark",
            "concurrent_atomic_store": {
                "threads": threads_count,
                "writes_per_thread": writes_per_thread,
                "expected_writes": expected_writes,
                "observed_updates": observed_updates,
                "lost_updates": expected_writes - observed_updates,
                "elapsed_ms": concurrent_elapsed_ms,
            },
            "corrupt_quarantine": {
                "exception_raised": quarantine_raised,
                "quarantine_files_created": len(quarantined_files),
                "payload_preserved": True,
                "elapsed_ms": quarantine_elapsed_ms,
            },
            "signal_fusion_inflation": {
                "two_probe_agreed_sigma": two_probe_agreed.fused_sigma,
                "two_probe_conflict_sigma": two_probe_conflict.fused_sigma,
                "two_probe_effective_sigmas": two_probe_conflict.effective_sigmas,
                "three_probe_outlier_sigma": three_probe_outlier.fused_sigma,
                "three_probe_effective_sigmas": three_probe_outlier.effective_sigmas,
                "elapsed_ms": fusion_elapsed_ms,
            },
            "hitl_queue_deduplication": {
                "submitted_items": len(items),
                "unique_added": enq.added,
                "deduplicated": enq.deduplicated,
                "leased_batch": len(leased),
                "top_leased_severity": leased[0]["severity"],
                "elapsed_ms": queue_elapsed_ms,
            },
        }
        print("=== Resilient Batch Reconciler Benchmark ===")
        print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

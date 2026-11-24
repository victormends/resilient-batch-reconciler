"""End-to-end local runnable demo exercising all 4 resilient-batch-reconciler modules."""

from __future__ import annotations

import json
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from reconciler import (
    AtomicJsonStore,
    HitlRemediationQueue,
    ProbeReading,
    RateLimitConfig,
    RateLimiter,
    RemediationItem,
    StageCheckpoint,
    TemplateHashCache,
    calibrate_median_durations,
    compute_incremental_window,
    estimate_fleet_completion,
    fuse_probe_readings,
)


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="reconciler-demo-") as tmp:
        root = Path(tmp)
        now = datetime(2026, 11, 24, 12, 0, 0, tzinfo=timezone.utc)

        # 1. Multi-probe inverse-variance fusion (agreeing vs 2-probe conflict)
        agreed = fuse_probe_readings(
            [
                ProbeReading("ebpf_kernel", 41.2, sigma=1.1, role="primary", metric="p95_ms"),
                ProbeReading("envoy_sidecar", 43.0, sigma=2.4, role="secondary", metric="p95_ms"),
            ],
            conflict_threshold=15.0,
        )
        conflicted = fuse_probe_readings(
            [
                ProbeReading("ebpf_kernel", 40.0, sigma=1.0, role="primary", metric="p95_ms"),
                ProbeReading("synthetic_canary", 95.0, sigma=2.5, role="secondary", metric="p95_ms"),
            ],
            conflict_threshold=15.0,
        )
        assert agreed is not None and conflicted is not None

        # 2. Monotonic asset evidence ledger (degraded second probe retains confirmed fields)
        store = AtomicJsonStore(root / "inventory.json")
        store.reconcile_asset(
            "prod-us-east-1/checkout-01",
            {
                "region": "us-east-1",
                "owner_team": "checkout-sre",
                "latency_ms": agreed.fused_value,
                "tags": ["tier-1", "postgres"],
            },
            source="ebpf-reconciler",
            observed_at="2026-11-24T11:55:00Z",
        )
        merged = store.reconcile_asset(
            "prod-us-east-1/checkout-01",
            {
                "region": None,
                "owner_team": "",
                "latency_ms": conflicted.fused_value,
                "tags": ["high-traffic"],
            },
            source="degraded-canary",
            observed_at="2026-11-24T12:00:00Z",
        )

        # 3. Persisted rate limiter + canary tripwire + deduplicated HITL queue
        limiter = RateLimiter(
            root / "rate_limit.json",
            config=RateLimitConfig(daily_cap=100, session_cap=20),
            clock=lambda: now,
            sleep_fn=lambda _s: None,
        )
        acq_ok = limiter.acquire()
        signal = limiter.check_canary_signal(status_code=429, payload_text="Rate limit exceeded")
        assert signal is not None

        queue = HitlRemediationQueue(root / "remediation_queue.json", clock=lambda: now)
        enqueued = queue.enqueue_remediations(
            [
                RemediationItem(
                    asset_id="prod-us-east-1/checkout-01",
                    action="shift_traffic",
                    reason="2-probe p95 conflict detected",
                    severity="high",
                ),
                RemediationItem(
                    asset_id="prod-us-east-1/checkout-01",
                    action="shift_traffic",
                    reason="duplicate alert collapsed",
                    severity="high",
                ),
            ],
            actor="demo-runner",
        )
        leased = queue.drain_batch(limit=5, reviewer="oncall-sre")

        # 4. Incremental overlap window + heap fleet ETA + spec-hash cache
        window = compute_incremental_window(now - timedelta(hours=1), now=now, overlap_buffer_seconds=1800)
        durations = calibrate_median_durations({"cluster-a": [12.0, 14.0], "cluster-b": [30.0, 28.0], "cluster-c": [18.0]})
        eta = estimate_fleet_completion(["cluster-a", "cluster-b", "cluster-c"], durations, concurrency=2, now_ts=1_700_000_000.0)
        chk = StageCheckpoint(root / "checkpoints.json", clock=lambda: now)
        chk.mark_item_done("triage", "cluster-a")
        cache = TemplateHashCache(root / "spec_cache.json", default_ttl_seconds=3600.0)
        cache.set("cluster-a", {"status": "cached"}, spec="v1-prompt")
        cached_hit = cache.get("cluster-a", spec="v1-prompt")
        cache_invalidated = cache.get("cluster-a", spec="v2-prompt") is None
        assert cached_hit == {"status": "cached"} and cache_invalidated

        summary = {
            "fusion_agreed": {"value": agreed.fused_value, "sigma": agreed.fused_sigma, "confidence": agreed.confidence},
            "fusion_conflict": {
                "conflict_flag": conflicted.conflict_flag,
                "effective_sigmas": conflicted.effective_sigmas,
                "fused_sigma": conflicted.fused_sigma,
                "confidence": conflicted.confidence,
            },
            "ledger_retained_fields": merged["retained_fields"],
            "ledger_region_preserved": merged["region"],
            "rate_limit_initial_allowed": acq_ok.allowed,
            "canary_signal_detected": signal,
            "queue_added": enqueued.added,
            "queue_deduplicated": enqueued.deduplicated,
            "queue_leased_count": len(leased),
            "incremental_window_seconds": window.window_seconds,
            "fleet_eta_seconds": eta.estimated_remaining_seconds,
            "pending_after_checkpoint": chk.filter_pending("triage", ["cluster-a", "cluster-b"]),
        }
        print("=== Resilient Batch Reconciler End-to-End Demo ===")
        print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

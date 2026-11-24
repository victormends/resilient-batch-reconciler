"""Tests for persisted canary rate limiting and deduplicated HITL remediation queue."""

from __future__ import annotations

import random
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from reconciler.rate_limiter import RateLimitConfig, RateLimiter
from reconciler.remediation_queue import (
    HitlRemediationQueue,
    RemediationItem,
    drain_batch,
    enqueue_remediations,
)


class FakeClock:
    def __init__(self, initial: datetime) -> None:
        self.now = initial

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **kwargs: float) -> None:
        self.now = self.now + timedelta(**kwargs)


def test_rate_limiter_caps_batch_pacing_and_exponential_backoff(tmp_path: Path) -> None:
    state_path = tmp_path / "limiter_state.json"
    clock = FakeClock(datetime(2026, 11, 24, 12, 0, 0, tzinfo=timezone.utc))
    sleeps = []
    cfg = RateLimitConfig(
        daily_cap=5,
        session_cap=3,
        batch_size=2,
        batch_pause_min_sec=10.0,
        batch_pause_max_sec=10.0,
        inter_action_delay_min_sec=1.0,
        inter_action_delay_max_sec=1.0,
        backoff_base_sec=2.0,
        backoff_max_sec=10.0,
    )
    rl = RateLimiter(
        state_path,
        cfg,
        clock=clock,
        sleep_fn=lambda sec: sleeps.append(sec),
        rng=random.Random(42),
    )

    d1 = rl.acquire()
    assert d1.allowed is True
    assert d1.delay_applied_sec == 1.0
    rl.record_success("asset-1")

    d2 = rl.acquire()
    assert d2.allowed is True
    assert d2.delay_applied_sec == 1.0
    rl.record_success("asset-2")

    # 3rd action hits session_count == 2 -> batch pause triggered
    d3 = rl.acquire()
    assert d3.allowed is True
    assert d3.delay_applied_sec == 10.0
    rl.record_success("asset-3")

    # 4th action blocked by session_cap == 3
    d4 = rl.acquire()
    assert d4.allowed is False
    assert d4.reason == "session_cap"

    # Exponential backoff: 2 -> 4 -> 8 -> capped 10
    assert rl.record_error("timeout-1") == 2.0
    assert rl.record_error("timeout-2") == 4.0
    assert rl.record_error("timeout-3") == 8.0
    assert rl.record_error("timeout-4") == 10.0
    assert sleeps[-4:] == [2.0, 4.0, 8.0, 10.0]

    # New session hits daily cap (3 already used today out of 5)
    rl2 = RateLimiter(state_path, cfg, clock=clock, sleep_fn=lambda s: None)
    assert rl2.acquire().allowed is True
    rl2.record_success("asset-4")
    assert rl2.acquire().allowed is True
    rl2.record_success("asset-5")
    d_daily = rl2.acquire()
    assert d_daily.allowed is False
    assert d_daily.reason == "daily_cap"


def test_canary_tripwire_persists_cooldown_across_restart_and_midnight_rollover(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "canary_state.json"
    clock = FakeClock(datetime(2026, 11, 24, 23, 50, 0, tzinfo=timezone.utc))
    cfg = RateLimitConfig(canary_cooldown_sec=3600.0)
    rl = RateLimiter(state_path, cfg, clock=clock, sleep_fn=lambda s: None)

    sig = rl.check_canary_signal(
        status_code=200,
        endpoint="/api/v1/remediate",
        payload_text="Error: ThrottlingException - rate exceeded",
    )
    assert sig == "payload_signal:throttlingexception"
    rl.trigger_canary_cooldown(sig)

    assert rl.check_canary_signal(status_code=429) == "http_status:429"
    assert rl.check_canary_signal(endpoint="/internal/circuit-open") == "endpoint_signal:/circuit-open"
    assert rl.check_canary_signal(headers={"X-Canary-Tripwire": "true"}) == "header_signal:x-canary-tripwire"

    # Advance 20 minutes across UTC midnight: daily_count rolls over, cooldown MUST stay active
    clock.advance(minutes=20)
    rl_restarted = RateLimiter(state_path, cfg, clock=clock, sleep_fn=lambda s: None)
    decision = rl_restarted.acquire()
    assert decision.allowed is False
    assert decision.reason == "canary_cooldown"
    assert decision.cooldown_remaining_sec == pytest.approx(2400.0, abs=1.0)

    # Advance past remaining 40+ minutes -> cooldown expires cleanly
    clock.advance(minutes=41)
    assert rl_restarted.acquire().allowed is True


def test_hitl_queue_dedup_priority_drain_and_concurrent_leasing(tmp_path: Path) -> None:
    queue_path = tmp_path / "remediation_queue.json"
    res1 = enqueue_remediations(
        queue_path,
        [
            RemediationItem("node-a", "restart_agent", "high mem", severity="low"),
            RemediationItem("node-b", "drain_node", "oom loop", severity="critical"),
            {"asset_id": "node-b", "action": "drain_node", "reason": "dup in batch", "severity": "critical"},
            RemediationItem("node-c", "rotate_cert", "expiring soon", severity="high"),
        ],
    )
    assert res1.added == 3
    assert res1.deduplicated == 1
    assert res1.pending_total == 3

    # Duplicate active item across subsequent enqueue is still deduplicated
    res2 = enqueue_remediations(
        queue_path,
        [RemediationItem("node-b", "drain_node", "repeat drift", severity="critical")],
    )
    assert res2.added == 0
    assert res2.deduplicated == 1

    # Priority drain: critical comes before high and low
    batch = drain_batch(queue_path, limit=2, reviewer="sre-alice")
    assert [item["asset_id"] for item in batch] == ["node-b", "node-c"]
    assert all(item["status"] == "leased" for item in batch)

    queue = HitlRemediationQueue(queue_path)
    pending = queue.list_items(status="pending")
    assert len(pending) == 1
    assert pending[0]["asset_id"] == "node-a"

    # HITL safety: pending item cannot skip directly to executed
    with pytest.raises(ValueError, match="requires HITL"):
        queue.resolve_item(pending[0]["remediation_id"], decision="executed", actor="bot")

    # Approve and execute leased critical item
    crit_id = batch[0]["remediation_id"]
    queue.resolve_item(crit_id, decision="approved", actor="sre-alice", note="verified healthy canary")
    queue.resolve_item(crit_id, decision="executed", actor="remediation-runner")

    # Once terminal (executed), new drift on same dedup_key can enqueue again
    res3 = queue.enqueue_remediations(
        [RemediationItem("node-b", "drain_node", "new incident next day", severity="critical")]
    )
    assert res3.added == 1


def test_hitl_queue_concurrent_drain_never_double_leases(tmp_path: Path) -> None:
    queue_path = tmp_path / "concurrent_queue.json"
    items = [
        RemediationItem(f"asset-{idx:02d}", "scale_pool", f"drift-{idx}", severity="medium")
        for idx in range(20)
    ]
    enqueue_remediations(queue_path, items)
    queue = HitlRemediationQueue(queue_path)

    with ThreadPoolExecutor(max_workers=5) as pool:
        batches = list(pool.map(lambda wid: queue.drain_batch(limit=5, reviewer=f"op-{wid}"), range(5)))

    leased_ids = [item["remediation_id"] for batch in batches for item in batch]
    assert len(leased_ids) == 20
    assert len(set(leased_ids)) == 20
    assert queue.stats()["pending"] == 0
    assert queue.stats()["leased"] == 20

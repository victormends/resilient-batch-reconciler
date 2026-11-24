"""Decoupled Human-in-the-Loop (HITL) remediation queue with deduplication and audit trail."""

from __future__ import annotations

import copy
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Union

from .state_store import AtomicJsonStore

SEVERITY_RANK: Dict[str, int] = {
    "critical": 0,
    "high": 1,
    "medium": 2,
    "low": 3,
}

ACTIVE_STATUSES = frozenset({"pending", "leased", "approved"})
TERMINAL_STATUSES = frozenset({"executed", "rejected"})


def _utc_now_iso(clock: Optional[Callable[[], datetime]] = None) -> str:
    dt = clock() if clock else datetime.now(timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class RemediationItem:
    """Proposed infrastructure remediation waiting for bounded HITL review."""

    asset_id: str
    action: str
    reason: str
    severity: str = "medium"
    dedup_key: Optional[str] = None
    payload: Dict[str, Any] = field(default_factory=dict)
    requires_human_approval: bool = True

    def normalized_dedup_key(self) -> str:
        if self.dedup_key and self.dedup_key.strip():
            return self.dedup_key.strip()
        return f"{self.asset_id}::{self.action}"


@dataclass(frozen=True)
class EnqueueResult:
    """Summary returned by ``enqueue_remediations``."""

    added: int
    deduplicated: int
    pending_total: int
    added_ids: List[str]


def _coerce_item(raw: Union[RemediationItem, Mapping[str, Any]]) -> RemediationItem:
    if isinstance(raw, RemediationItem):
        item = raw
    elif isinstance(raw, Mapping):
        item = RemediationItem(
            asset_id=str(raw["asset_id"]),
            action=str(raw["action"]),
            reason=str(raw.get("reason", "")),
            severity=str(raw.get("severity", "medium")).lower(),
            dedup_key=raw.get("dedup_key"),
            payload=dict(raw.get("payload") or {}),
            requires_human_approval=bool(raw.get("requires_human_approval", True)),
        )
    else:
        raise TypeError(f"Unsupported remediation item type: {type(raw)!r}")

    if not item.asset_id.strip():
        raise ValueError("RemediationItem asset_id must be non-empty")
    if not item.action.strip():
        raise ValueError("RemediationItem action must be non-empty")
    if item.severity not in SEVERITY_RANK:
        raise ValueError(f"Invalid severity {item.severity!r}; expected one of {sorted(SEVERITY_RANK)}")
    return item


class HitlRemediationQueue:
    """Thread/process-safe persistent HITL remediation queue with deduplication and leasing."""

    def __init__(
        self,
        queue_path: Union[str, Path],
        *,
        clock: Optional[Callable[[], datetime]] = None,
    ) -> None:
        self.store = AtomicJsonStore(queue_path)
        self._clock = clock

    def enqueue_remediations(
        self,
        items: Sequence[Union[RemediationItem, Mapping[str, Any]]],
        *,
        actor: str = "reconciler",
    ) -> EnqueueResult:
        """Enqueue proposed remediations, deduplicating against active items."""
        coerced = [_coerce_item(raw) for raw in items]
        ts = _utc_now_iso(self._clock)
        outcome: Dict[str, Any] = {}

        def _mutate(doc: Dict[str, Any]) -> Dict[str, Any]:
            stored: Dict[str, Dict[str, Any]] = doc.setdefault("items", {})
            seq = int(doc.get("sequence", 0))

            active_by_dedup: Dict[str, str] = {}
            for rem_id, entry in stored.items():
                if entry.get("status") in ACTIVE_STATUSES:
                    active_by_dedup[entry["dedup_key"]] = rem_id

            added_ids: List[str] = []
            deduplicated = 0

            for item in coerced:
                dkey = item.normalized_dedup_key()
                if dkey in active_by_dedup:
                    existing_id = active_by_dedup[dkey]
                    existing_entry = stored[existing_id]
                    existing_entry["duplicate_hits"] = int(existing_entry.get("duplicate_hits", 0)) + 1
                    existing_entry["last_duplicate_seen_at"] = ts
                    existing_entry.setdefault("audit_trail", []).append(
                        {
                            "at": ts,
                            "actor": actor,
                            "event": "deduplicated",
                            "status": existing_entry["status"],
                            "note": item.reason,
                        }
                    )
                    deduplicated += 1
                    continue

                seq += 1
                digest = hashlib.sha256(f"{dkey}:{ts}:{seq}".encode("utf-8")).hexdigest()[:12]
                rem_id = f"rem-{digest}"
                record: Dict[str, Any] = {
                    "remediation_id": rem_id,
                    "sequence": seq,
                    "asset_id": item.asset_id,
                    "action": item.action,
                    "severity": item.severity,
                    "reason": item.reason,
                    "dedup_key": dkey,
                    "payload": copy.deepcopy(item.payload),
                    "requires_human_approval": item.requires_human_approval,
                    "status": "pending",
                    "enqueued_at": ts,
                    "updated_at": ts,
                    "leased_by": None,
                    "duplicate_hits": 0,
                    "audit_trail": [
                        {
                            "at": ts,
                            "actor": actor,
                            "event": "enqueued",
                            "status": "pending",
                            "note": item.reason,
                        }
                    ],
                }
                stored[rem_id] = record
                active_by_dedup[dkey] = rem_id
                added_ids.append(rem_id)

            doc["sequence"] = seq
            pending_total = sum(1 for v in stored.values() if v.get("status") == "pending")
            outcome["result"] = EnqueueResult(
                added=len(added_ids),
                deduplicated=deduplicated,
                pending_total=pending_total,
                added_ids=added_ids,
            )
            return doc

        self.store.update(_mutate)
        return outcome["result"]

    def drain_batch(
        self,
        limit: int = 10,
        *,
        reviewer: str = "hitl-operator",
        auto_approve: bool = False,
        severities: Optional[Sequence[str]] = None,
        asset_ids: Optional[Sequence[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Atomically lease (or approve) up to ``limit`` pending items prioritized by severity."""
        if limit < 0:
            raise ValueError("limit must be non-negative")
        if limit == 0:
            return []

        sev_filter = {s.lower() for s in severities} if severities else None
        asset_filter = set(asset_ids) if asset_ids else None
        ts = _utc_now_iso(self._clock)
        drained: List[Dict[str, Any]] = []

        def _mutate(doc: Dict[str, Any]) -> Dict[str, Any]:
            stored: Dict[str, Dict[str, Any]] = doc.setdefault("items", {})
            candidates = [
                entry
                for entry in stored.values()
                if entry.get("status") == "pending"
                and (sev_filter is None or entry.get("severity") in sev_filter)
                and (asset_filter is None or entry.get("asset_id") in asset_filter)
            ]
            candidates.sort(
                key=lambda e: (
                    SEVERITY_RANK.get(str(e.get("severity")), 99),
                    str(e.get("enqueued_at", "")),
                    int(e.get("sequence", 0)),
                )
            )
            selected = candidates[:limit]
            target_status = "approved" if auto_approve else "leased"
            event_name = "auto_approved" if auto_approve else "leased"

            for entry in selected:
                entry["status"] = target_status
                entry["leased_by"] = reviewer
                entry["updated_at"] = ts
                entry.setdefault("audit_trail", []).append(
                    {
                        "at": ts,
                        "actor": reviewer,
                        "event": event_name,
                        "status": target_status,
                        "note": "",
                    }
                )
                drained.append(copy.deepcopy(entry))
            return doc

        self.store.update(_mutate)
        return drained

    def resolve_item(
        self,
        remediation_id: str,
        *,
        decision: str,
        actor: str,
        note: str = "",
    ) -> Dict[str, Any]:
        """Transition a remediation item across HITL review/execution states."""
        valid_decisions = {"approved", "executed", "rejected"}
        if decision not in valid_decisions:
            raise ValueError(f"decision must be one of {sorted(valid_decisions)}, got {decision!r}")

        ts = _utc_now_iso(self._clock)
        holder: Dict[str, Any] = {}

        def _mutate(doc: Dict[str, Any]) -> Dict[str, Any]:
            stored: Dict[str, Dict[str, Any]] = doc.setdefault("items", {})
            entry = stored.get(remediation_id)
            if entry is None:
                raise KeyError(f"Unknown remediation_id: {remediation_id!r}")

            current = entry.get("status")
            if current in TERMINAL_STATUSES:
                raise ValueError(
                    f"Cannot transition remediation {remediation_id!r} from terminal status {current!r}"
                )
            if (
                decision == "executed"
                and entry.get("requires_human_approval", True)
                and current == "pending"
            ):
                raise ValueError(
                    f"Remediation {remediation_id!r} requires HITL lease/approval before execution"
                )
            if decision == "approved" and current == "approved":
                holder["entry"] = copy.deepcopy(entry)
                return doc

            entry["status"] = decision
            entry["updated_at"] = ts
            if decision == "approved":
                entry["approved_by"] = actor
            elif decision == "executed":
                entry["executed_by"] = actor
            elif decision == "rejected":
                entry["rejected_by"] = actor

            entry.setdefault("audit_trail", []).append(
                {
                    "at": ts,
                    "actor": actor,
                    "event": decision,
                    "status": decision,
                    "note": note,
                }
            )
            holder["entry"] = copy.deepcopy(entry)
            return doc

        self.store.update(_mutate)
        return holder["entry"]

    def get_item(self, remediation_id: str) -> Optional[Dict[str, Any]]:
        doc = self.store.load()
        entry = (doc.get("items") or {}).get(remediation_id)
        return copy.deepcopy(entry) if entry else None

    def list_items(self, *, status: Optional[str] = None) -> List[Dict[str, Any]]:
        doc = self.store.load()
        items = list((doc.get("items") or {}).values())
        if status is not None:
            items = [item for item in items if item.get("status") == status]
        items.sort(key=lambda e: int(e.get("sequence", 0)))
        return copy.deepcopy(items)

    def stats(self) -> Dict[str, int]:
        doc = self.store.load()
        counts: Dict[str, int] = {
            "pending": 0,
            "leased": 0,
            "approved": 0,
            "executed": 0,
            "rejected": 0,
            "total": 0,
        }
        for entry in (doc.get("items") or {}).values():
            st = str(entry.get("status", "pending"))
            counts[st] = counts.get(st, 0) + 1
            counts["total"] += 1
        return counts


def enqueue_remediations(
    queue_path: Union[str, Path],
    items: Sequence[Union[RemediationItem, Mapping[str, Any]]],
    *,
    actor: str = "reconciler",
    clock: Optional[Callable[[], datetime]] = None,
) -> EnqueueResult:
    """Functional helper to deduplicate and enqueue HITL remediations."""
    return HitlRemediationQueue(queue_path, clock=clock).enqueue_remediations(items, actor=actor)


def drain_batch(
    queue_path: Union[str, Path],
    limit: int = 10,
    *,
    reviewer: str = "hitl-operator",
    auto_approve: bool = False,
    severities: Optional[Sequence[str]] = None,
    asset_ids: Optional[Sequence[str]] = None,
    clock: Optional[Callable[[], datetime]] = None,
) -> List[Dict[str, Any]]:
    """Functional helper to atomically drain up to ``limit`` pending HITL remediations."""
    return HitlRemediationQueue(queue_path, clock=clock).drain_batch(
        limit=limit,
        reviewer=reviewer,
        auto_approve=auto_approve,
        severities=severities,
        asset_ids=asset_ids,
    )

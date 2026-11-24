"""Pipeline orchestration utilities: incremental windows, heap ETA simulation, checkpoints, and spec-hash cache."""

from __future__ import annotations

import copy
import hashlib
import heapq
import json
import statistics
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Union

from .state_store import AtomicJsonStore


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_utc(value: Optional[Union[datetime, str]]) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    raw = str(value).strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _to_iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class IncrementalWindow:
    """Bounded lookback window including overlap buffer for delayed telemetry ingestion."""

    window_seconds: int
    start_at: datetime
    end_at: datetime
    reason: str
    clamped: bool = False


def compute_incremental_window(
    last_successful_at: Optional[Union[datetime, str]],
    *,
    now: Optional[datetime] = None,
    default_seconds: int = 86_400,
    min_seconds: int = 60,
    max_seconds: int = 604_800,
    overlap_buffer_seconds: int = 7_200,
) -> IncrementalWindow:
    """Compute an incremental reconciliation lookback window with configurable overlap buffer."""
    if min_seconds <= 0:
        raise ValueError("min_seconds must be positive")
    if max_seconds < min_seconds:
        raise ValueError("max_seconds must be >= min_seconds")
    if overlap_buffer_seconds < 0:
        raise ValueError("overlap_buffer_seconds must be non-negative")

    end_at = _parse_utc(now) or _utc_now()
    last_dt = _parse_utc(last_successful_at)

    if last_dt is None:
        clamped_sec = min(max(int(default_seconds), min_seconds), max_seconds)
        return IncrementalWindow(
            window_seconds=clamped_sec,
            start_at=end_at - timedelta(seconds=clamped_sec),
            end_at=end_at,
            reason="no previous successful checkpoint",
            clamped=clamped_sec != int(default_seconds),
        )

    elapsed = int((end_at - last_dt).total_seconds())
    if elapsed < 0:
        return IncrementalWindow(
            window_seconds=min_seconds,
            start_at=end_at - timedelta(seconds=min_seconds),
            end_at=end_at,
            reason="checkpoint timestamp is in the future",
            clamped=True,
        )

    buffered = elapsed + overlap_buffer_seconds
    clamped_sec = min(max(buffered, min_seconds), max_seconds)
    clamped = clamped_sec != buffered
    if buffered > max_seconds:
        reason = f"clamped to max lookback ({max_seconds}s) including {overlap_buffer_seconds}s overlap buffer"
    else:
        reason = f"since last successful checkpoint (+{overlap_buffer_seconds}s overlap buffer)"

    return IncrementalWindow(
        window_seconds=clamped_sec,
        start_at=end_at - timedelta(seconds=clamped_sec),
        end_at=end_at,
        reason=reason,
        clamped=clamped,
    )


def calibrate_median_durations(
    historical_samples: Mapping[str, Sequence[float]],
    fallback_durations: Optional[Mapping[str, float]] = None,
    *,
    default_duration_sec: float = 300.0,
    max_samples: int = 5,
    min_duration_sec: float = 1.0,
    max_duration_sec: float = 86_400.0,
) -> Dict[str, float]:
    """Calibrate per-target task durations using the median of recent positive samples."""
    calibrated: Dict[str, float] = dict(fallback_durations or {})
    all_keys = set(calibrated.keys()) | set(historical_samples.keys())

    for key in all_keys:
        raw_samples = list(historical_samples.get(key) or ())
        valid = [
            min(max(float(val), min_duration_sec), max_duration_sec)
            for val in raw_samples[:max_samples]
            if float(val) > 0.0
        ]
        if valid:
            calibrated[key] = float(statistics.median(valid))
        elif key not in calibrated:
            calibrated[key] = float(default_duration_sec)

    return calibrated


@dataclass(frozen=True)
class FleetEtaEstimate:
    """Min-heap simulation result for concurrent fleet completion."""

    estimated_remaining_seconds: float
    estimated_finish_at: datetime
    margin_seconds: float
    completed_count: int
    active_count: int
    queued_count: int
    total_count: int
    worker_slot_loads: List[float]


def estimate_fleet_completion(
    tasks: Sequence[str],
    durations_sec: Mapping[str, float],
    *,
    concurrency: int = 4,
    stagger_sec: float = 0.0,
    completed_tasks: Optional[Iterable[str]] = None,
    active_started_at: Optional[Mapping[str, float]] = None,
    now_ts: Optional[float] = None,
    started_at_ts: Optional[float] = None,
    default_duration_sec: float = 300.0,
) -> FleetEtaEstimate:
    """Simulate parallel worker slot completion using a min-heap over median task durations."""
    if concurrency < 1:
        raise ValueError("concurrency must be >= 1")
    if stagger_sec < 0.0:
        raise ValueError("stagger_sec must be non-negative")

    current_ts = float(time.time() if now_ts is None else now_ts)
    start_ts = float(current_ts if started_at_ts is None else started_at_ts)
    elapsed = max(0.0, current_ts - start_ts)

    completed_set = set(completed_tasks or ())
    active_map = dict(active_started_at or {})
    remaining = [task for task in tasks if task not in completed_set]

    now_dt = datetime.fromtimestamp(current_ts, tz=timezone.utc)
    if not remaining:
        return FleetEtaEstimate(
            estimated_remaining_seconds=0.0,
            estimated_finish_at=now_dt,
            margin_seconds=0.0,
            completed_count=len(tasks),
            active_count=0,
            queued_count=0,
            total_count=len(tasks),
            worker_slot_loads=[],
        )

    orig_index = {task: idx for idx, task in enumerate(tasks)}
    active_tasks = [task for task in remaining if task in active_map]
    queued_tasks = [task for task in remaining if task not in active_map]

    slots: List[float] = []
    for task in active_tasks:
        dur = max(1.0, float(durations_sec.get(task, default_duration_sec)))
        el_task = max(0.0, current_ts - float(active_map[task]))
        if el_task < dur:
            rem_task = max(min(dur * 0.10, 5.0), dur - el_task)
        else:
            rem_task = max(min(dur * 0.15, 60.0), 1.0)
        if len(slots) < concurrency:
            slots.append(rem_task)

    sim_queue = list(queued_tasks)
    slot_cap = min(concurrency, len(remaining))
    while len(slots) < slot_cap and sim_queue:
        next_task = sim_queue.pop(0)
        idx = orig_index.get(next_task, concurrency)
        stagger_wait = max(0.0, idx * stagger_sec - elapsed) if idx < concurrency else 0.0
        dur = max(0.0, float(durations_sec.get(next_task, default_duration_sec)))
        slots.append(stagger_wait + dur)

    heapq.heapify(slots)
    for q_task in sim_queue:
        earliest_free = heapq.heappop(slots)
        dur = max(0.0, float(durations_sec.get(q_task, default_duration_sec)))
        heapq.heappush(slots, earliest_free + dur)

    est_remaining = max(slots) if slots else 0.0
    margin = round(max(1.0, est_remaining * 0.15), 2) if est_remaining > 0 else 0.0
    finish_dt = datetime.fromtimestamp(current_ts + est_remaining, tz=timezone.utc)

    return FleetEtaEstimate(
        estimated_remaining_seconds=round(est_remaining, 3),
        estimated_finish_at=finish_dt,
        margin_seconds=margin,
        completed_count=len(tasks) - len(remaining),
        active_count=len(active_tasks),
        queued_count=len(queued_tasks),
        total_count=len(tasks),
        worker_slot_loads=sorted(round(val, 3) for val in slots),
    )


class StageCheckpoint:
    """Idempotent multi-stage checkpoint backed by ``AtomicJsonStore``."""

    def __init__(
        self,
        checkpoint_path: Union[str, Path],
        *,
        clock: Optional[Callable[[], datetime]] = None,
    ) -> None:
        self.store = AtomicJsonStore(checkpoint_path)
        self._clock = clock or _utc_now

    def mark_item_done(
        self,
        stage: str,
        item_id: str,
        *,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        return self.mark_items_done(stage, [item_id], metadata=metadata)

    def mark_items_done(
        self,
        stage: str,
        item_ids: Sequence[str],
        *,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        ts = _to_iso_utc(self._clock())

        def _mutate(doc: Dict[str, Any]) -> Dict[str, Any]:
            stages = doc.setdefault("stages", {})
            st = stages.setdefault(
                stage,
                {"done_items": {}, "completed": False, "completed_at": None, "updated_at": ts},
            )
            done = st.setdefault("done_items", {})
            for item_id in item_ids:
                if item_id not in done:
                    done[item_id] = {
                        "completed_at": ts,
                        "metadata": copy.deepcopy(dict(metadata)) if metadata else {},
                    }
            st["updated_at"] = ts
            return doc

        return self.store.update(_mutate)

    def is_item_done(self, stage: str, item_id: str) -> bool:
        doc = self.store.load()
        stage_data = (doc.get("stages") or {}).get(stage) or {}
        return item_id in (stage_data.get("done_items") or {})

    def filter_pending(self, stage: str, items: Sequence[str]) -> List[str]:
        """Return only items not yet recorded as completed in ``stage``."""
        doc = self.store.load()
        stage_data = (doc.get("stages") or {}).get(stage) or {}
        done = set((stage_data.get("done_items") or {}).keys())
        return [item for item in items if item not in done]

    def mark_stage_completed(
        self,
        stage: str,
        *,
        completed_at: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        ts = _to_iso_utc(completed_at or self._clock())

        def _mutate(doc: Dict[str, Any]) -> Dict[str, Any]:
            stages = doc.setdefault("stages", {})
            st = stages.setdefault(
                stage,
                {"done_items": {}, "completed": True, "completed_at": ts, "updated_at": ts},
            )
            st["completed"] = True
            st["completed_at"] = ts
            st["updated_at"] = ts
            return doc

        return self.store.update(_mutate)

    def is_stage_completed(self, stage: str) -> bool:
        doc = self.store.load()
        stage_data = (doc.get("stages") or {}).get(stage) or {}
        return bool(stage_data.get("completed", False))

    def save_last_successful_run(
        self,
        *,
        completed_at: Optional[datetime] = None,
    ) -> str:
        ts = _to_iso_utc(completed_at or self._clock())

        def _mutate(doc: Dict[str, Any]) -> Dict[str, Any]:
            doc["last_successful_run_at"] = ts
            return doc

        self.store.update(_mutate)
        return ts

    def last_successful_run_at(self) -> Optional[str]:
        doc = self.store.load()
        val = doc.get("last_successful_run_at")
        return str(val) if val else None

    def reset_stage(self, stage: str) -> Dict[str, Any]:
        def _mutate(doc: Dict[str, Any]) -> Dict[str, Any]:
            stages = doc.setdefault("stages", {})
            stages.pop(stage, None)
            return doc

        return self.store.update(_mutate)

    def summary(self) -> Dict[str, Any]:
        doc = self.store.load()
        stages_summary: Dict[str, Any] = {}
        for name, st in (doc.get("stages") or {}).items():
            stages_summary[name] = {
                "done_count": len(st.get("done_items") or {}),
                "completed": bool(st.get("completed", False)),
                "completed_at": st.get("completed_at"),
            }
        return {
            "last_successful_run_at": doc.get("last_successful_run_at"),
            "stages": stages_summary,
        }


class TemplateHashCache:
    """TTL cache that automatically invalidates entries when the SHA-256 spec/prompt changes."""

    def __init__(
        self,
        cache_path: Optional[Union[str, Path]] = None,
        *,
        default_ttl_seconds: Optional[float] = None,
        clock_ts: Optional[Callable[[], float]] = None,
    ) -> None:
        if default_ttl_seconds is not None and default_ttl_seconds <= 0:
            raise ValueError("default_ttl_seconds must be positive")
        self.store: Optional[AtomicJsonStore] = (
            AtomicJsonStore(cache_path) if cache_path is not None else None
        )
        self._memory: Dict[str, Any] = {}
        self.default_ttl_seconds = default_ttl_seconds
        self._clock_ts = clock_ts or time.time

    @staticmethod
    def hash_spec(spec: Union[str, Mapping[str, Any], Sequence[Any]]) -> str:
        """Compute a deterministic SHA-256 hex digest for a template, spec, or prompt."""
        if isinstance(spec, str):
            raw = spec
        else:
            raw = json.dumps(spec, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8", errors="ignore")).hexdigest()

    def _load_entries(self) -> Dict[str, Any]:
        if self.store is None:
            return self._memory
        doc = self.store.load()
        entries = doc.get("entries")
        return entries if isinstance(entries, dict) else {}

    def get(
        self,
        key: str,
        *,
        spec: Union[str, Mapping[str, Any], Sequence[Any]],
    ) -> Optional[Any]:
        """Return cached value if present, TTL-valid, and matching ``sha256(spec)``."""
        spec_hash = self.hash_spec(spec)
        entries = self._load_entries()
        entry = entries.get(key)
        if not isinstance(entry, dict):
            return None
        if entry.get("spec_hash") != spec_hash:
            return None
        expires_at = entry.get("expires_at_ts")
        if expires_at is not None and float(self._clock_ts()) >= float(expires_at):
            return None
        return copy.deepcopy(entry.get("value"))

    def set(
        self,
        key: str,
        value: Any,
        *,
        spec: Union[str, Mapping[str, Any], Sequence[Any]],
        ttl_seconds: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Store a value bound to ``sha256(spec)`` and optional TTL."""
        ttl = self.default_ttl_seconds if ttl_seconds is None else ttl_seconds
        if ttl is not None and ttl <= 0:
            raise ValueError("ttl_seconds must be positive")

        now_epoch = float(self._clock_ts())
        entry = {
            "value": copy.deepcopy(value),
            "spec_hash": self.hash_spec(spec),
            "created_at_ts": now_epoch,
            "expires_at_ts": (now_epoch + float(ttl)) if ttl is not None else None,
        }

        if self.store is None:
            self._memory[key] = entry
            return copy.deepcopy(entry)

        def _mutate(doc: Dict[str, Any]) -> Dict[str, Any]:
            entries = doc.setdefault("entries", {})
            entries[key] = entry
            return doc

        self.store.update(_mutate)
        return copy.deepcopy(entry)

    def invalidate(self, key: str) -> bool:
        """Remove a specific cache key."""
        if self.store is None:
            return self._memory.pop(key, None) is not None

        removed_holder = {"removed": False}

        def _mutate(doc: Dict[str, Any]) -> Dict[str, Any]:
            entries = doc.setdefault("entries", {})
            removed_holder["removed"] = entries.pop(key, None) is not None
            return doc

        self.store.update(_mutate)
        return removed_holder["removed"]

    def prune_expired(self) -> int:
        """Delete all entries whose TTL has elapsed."""
        now_epoch = float(self._clock_ts())
        if self.store is None:
            expired = [
                k
                for k, v in self._memory.items()
                if v.get("expires_at_ts") is not None and now_epoch >= float(v["expires_at_ts"])
            ]
            for k in expired:
                self._memory.pop(k, None)
            return len(expired)

        holder = {"count": 0}

        def _mutate(doc: Dict[str, Any]) -> Dict[str, Any]:
            entries = doc.setdefault("entries", {})
            expired = [
                k
                for k, v in entries.items()
                if isinstance(v, dict)
                and v.get("expires_at_ts") is not None
                and now_epoch >= float(v["expires_at_ts"])
            ]
            for k in expired:
                entries.pop(k, None)
            holder["count"] = len(expired)
            return doc

        self.store.update(_mutate)
        return holder["count"]

    def invalidate_mismatched_specs(
        self,
        active_spec: Union[str, Mapping[str, Any], Sequence[Any]],
    ) -> int:
        """Delete all cached entries whose ``spec_hash`` differs from ``sha256(active_spec)``."""
        active_hash = self.hash_spec(active_spec)
        if self.store is None:
            stale = [k for k, v in self._memory.items() if v.get("spec_hash") != active_hash]
            for k in stale:
                self._memory.pop(k, None)
            return len(stale)

        holder = {"count": 0}

        def _mutate(doc: Dict[str, Any]) -> Dict[str, Any]:
            entries = doc.setdefault("entries", {})
            stale = [
                k
                for k, v in entries.items()
                if not isinstance(v, dict) or v.get("spec_hash") != active_hash
            ]
            for k in stale:
                entries.pop(k, None)
            holder["count"] = len(stale)
            return doc

        self.store.update(_mutate)
        return holder["count"]

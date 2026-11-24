"""Atomic JSON persistence and monotonic SRE asset evidence ledger."""

from __future__ import annotations

import copy
import json
import logging
import os
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Mapping, Optional, Sequence, Union

import filelock

log = logging.getLogger(__name__)

_REGISTRY_LOCK = threading.Lock()
_PATH_THREAD_LOCKS: Dict[str, threading.RLock] = {}

_RESERVED_LEDGER_KEYS = frozenset(
    {
        "evidence_ledger",
        "fact_provenance",
        "retained_fields",
        "last_reconciled_at",
        "sources_seen",
        "observations_count",
    }
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _get_thread_lock(resolved_path: str) -> threading.RLock:
    with _REGISTRY_LOCK:
        lock = _PATH_THREAD_LOCKS.get(resolved_path)
        if lock is None:
            lock = threading.RLock()
            _PATH_THREAD_LOCKS[resolved_path] = lock
        return lock


def _is_missing(value: Any) -> bool:
    """Return True only when value represents absence, preserving 0, 0.0, and False."""
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0
    return False


def _merge_nested_monotonic(
    existing: Mapping[str, Any],
    incoming: Mapping[str, Any],
    retained_prefix: str,
    retained_out: List[str],
) -> Dict[str, Any]:
    merged: Dict[str, Any] = copy.deepcopy(dict(existing))
    for sub_key, new_val in incoming.items():
        qualified = f"{retained_prefix}.{sub_key}"
        if _is_missing(new_val):
            if sub_key in merged and not _is_missing(merged[sub_key]):
                retained_out.append(qualified)
            continue
        if isinstance(new_val, Mapping) and isinstance(merged.get(sub_key), Mapping):
            merged[sub_key] = _merge_nested_monotonic(
                merged[sub_key], new_val, qualified, retained_out
            )
        else:
            merged[sub_key] = copy.deepcopy(new_val)
    for old_sub_key, old_val in existing.items():
        if old_sub_key not in incoming and not _is_missing(old_val):
            retained_out.append(f"{retained_prefix}.{old_sub_key}")
    return merged


class AtomicJsonStore:
    """Cross-thread and cross-process atomic JSON document store.

    Combines an in-process reentrant thread lock with ``filelock.FileLock`` and
    unique PID/thread/nanosecond temporary files, retrying ``Path.replace()`` on
    transient Windows ``PermissionError``/``OSError`` lock contentions.
    """

    def __init__(
        self,
        path: Union[str, Path],
        *,
        lock_timeout: float = 30.0,
        max_retries: int = 15,
        retry_delay: float = 0.05,
        sleep_fn: Optional[Callable[[float], None]] = None,
    ) -> None:
        self.path = Path(path)
        self.lock_path = self.path.with_name(f"{self.path.name}.lock")
        self.lock_timeout = float(lock_timeout)
        self.max_retries = max(1, int(max_retries))
        self.retry_delay = max(0.0, float(retry_delay))
        self._sleep_fn = sleep_fn or time.sleep
        self._thread_lock = _get_thread_lock(str(self.path.resolve()))

    @contextmanager
    def _locked(self) -> Iterator[None]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._thread_lock:
            flock = filelock.FileLock(str(self.lock_path), timeout=self.lock_timeout)
            with flock:
                yield

    def _read_unlocked(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {}
        try:
            raw = self.path.read_text(encoding="utf-8-sig")
            if not raw.strip():
                return {}
            data = json.loads(raw)
            if isinstance(data, dict):
                return data
            log.warning("AtomicJsonStore expected dict JSON at %s; returning empty dict", self.path)
            return {}
        except (OSError, json.JSONDecodeError) as exc:
            log.warning("Failed to read JSON store %s: %s", self.path, exc)
            return {}

    def _write_unlocked(self, payload: Mapping[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        serialized = json.dumps(dict(payload), ensure_ascii=False, indent=2)
        tmp_name = f"{self.path.name}.{os.getpid()}.{threading.get_ident()}.{time.time_ns()}.tmp"
        tmp_path = self.path.with_name(tmp_name)
        try:
            with open(tmp_path, "w", encoding="utf-8") as fh:
                fh.write(serialized)
                fh.flush()
                try:
                    os.fsync(fh.fileno())
                except OSError:
                    pass

            for attempt in range(self.max_retries):
                try:
                    if not tmp_path.exists():
                        tmp_path.write_text(serialized, encoding="utf-8")
                    tmp_path.replace(self.path)
                    return
                except (PermissionError, OSError) as exc:
                    if attempt >= self.max_retries - 1:
                        raise
                    log.debug(
                        "Atomic replace blocked on %s (attempt %d/%d): %s",
                        self.path,
                        attempt + 1,
                        self.max_retries,
                        exc,
                    )
                    self._sleep_fn(self.retry_delay)
        finally:
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except OSError:
                    pass

    def load(self) -> Dict[str, Any]:
        """Load the persisted JSON dictionary under lock."""
        with self._locked():
            return self._read_unlocked()

    def save(
        self,
        data: Mapping[str, Any],
        *,
        merge_existing: bool = True,
    ) -> Dict[str, Any]:
        """Atomically persist ``data``, optionally merging with existing top-level keys."""
        with self._locked():
            if merge_existing:
                merged = self._read_unlocked()
                merged.update(copy.deepcopy(dict(data)))
            else:
                merged = copy.deepcopy(dict(data))
            self._write_unlocked(merged)
            return merged

    def update(
        self,
        mutator: Callable[[Dict[str, Any]], Optional[Dict[str, Any]]],
    ) -> Dict[str, Any]:
        """Atomically read, transform via ``mutator``, and persist the updated document."""
        with self._locked():
            current = self._read_unlocked()
            working = copy.deepcopy(current)
            result = mutator(working)
            updated = working if result is None else result
            if not isinstance(updated, dict):
                raise TypeError("Store mutator must return a dict or mutate in-place and return None")
            self._write_unlocked(updated)
            return updated

    def reconcile_asset(
        self,
        asset_id: str,
        observation: Mapping[str, Any],
        *,
        source: str = "probe",
        observed_at: Optional[str] = None,
        assets_key: str = "assets",
        monotonic_list_fields: Sequence[str] = ("tags", "endpoints", "probes_seen"),
    ) -> Dict[str, Any]:
        """Atomically reconcile an asset observation into ``assets[asset_id]``."""
        if not asset_id or not str(asset_id).strip():
            raise ValueError("asset_id must be a non-empty string")

        reconciled_holder: Dict[str, Any] = {}

        def _mutate(doc: Dict[str, Any]) -> Dict[str, Any]:
            bucket = doc.setdefault(assets_key, {})
            if not isinstance(bucket, dict):
                bucket = {}
                doc[assets_key] = bucket
            existing = bucket.get(asset_id)
            obs_payload = dict(observation)
            obs_payload.setdefault("asset_id", asset_id)
            merged = reconcile_asset_record(
                existing,
                obs_payload,
                source=source,
                observed_at=observed_at,
                monotonic_list_fields=monotonic_list_fields,
            )
            bucket[asset_id] = merged
            reconciled_holder["record"] = merged
            return doc

        self.update(_mutate)
        return reconciled_holder["record"]


def reconcile_asset_record(
    existing: Optional[Mapping[str, Any]],
    observation: Mapping[str, Any],
    *,
    source: str = "probe",
    observed_at: Optional[str] = None,
    monotonic_list_fields: Sequence[str] = ("tags", "endpoints", "probes_seen"),
    max_ledger_entries: Optional[int] = None,
) -> Dict[str, Any]:
    """Reconcile a new SRE asset observation monotonically against an existing record.

    Invariants:
    - Absent, empty, or ``None`` fields in ``observation`` NEVER overwrite previously
      confirmed non-empty values (numeric ``0``, ``0.0``, and ``False`` are valid values).
    - Every observation is appended to ``evidence_ledger`` with ``source`` and ``observed_at``.
    - Per-field provenance is tracked in ``fact_provenance``.
    """
    ts = observed_at or _utc_now_iso()
    old_record: Dict[str, Any] = copy.deepcopy(dict(existing)) if existing else {}
    merged: Dict[str, Any] = copy.deepcopy(old_record)
    provenance: Dict[str, Dict[str, Any]] = copy.deepcopy(
        dict(old_record.get("fact_provenance") or {})
    )
    ledger: List[Dict[str, Any]] = list(old_record.get("evidence_ledger") or [])
    retained: List[str] = []
    observed_fields: Dict[str, Any] = {}
    list_field_set = set(monotonic_list_fields)

    for key, new_val in observation.items():
        if key in _RESERVED_LEDGER_KEYS:
            continue
        if _is_missing(new_val):
            if key in merged and not _is_missing(merged[key]):
                retained.append(key)
            continue

        if key in list_field_set and isinstance(new_val, (list, tuple, set)):
            prev_list = merged.get(key) if isinstance(merged.get(key), list) else []
            combined = list(dict.fromkeys(list(prev_list) + list(new_val)))
            merged[key] = combined
        elif isinstance(new_val, Mapping) and isinstance(merged.get(key), Mapping):
            merged[key] = _merge_nested_monotonic(merged[key], new_val, key, retained)
        else:
            merged[key] = copy.deepcopy(new_val)

        observed_fields[key] = copy.deepcopy(new_val)
        prev_prov = provenance.get(key)
        if prev_prov:
            prev_prov["value"] = copy.deepcopy(merged[key])
            prev_prov["last_confirmed_at"] = ts
            prev_prov["last_source"] = source
            prev_prov["observations_count"] = int(prev_prov.get("observations_count", 1)) + 1
        else:
            provenance[key] = {
                "value": copy.deepcopy(merged[key]),
                "first_confirmed_at": ts,
                "last_confirmed_at": ts,
                "last_source": source,
                "observations_count": 1,
            }

    for key, old_val in old_record.items():
        if key in _RESERVED_LEDGER_KEYS or key in observation:
            continue
        if not _is_missing(old_val):
            retained.append(key)

    retained_sorted = sorted(set(retained))
    ledger.append(
        {
            "observed_at": ts,
            "source": source,
            "observed_fields": observed_fields,
            "retained_fields": retained_sorted,
        }
    )
    if max_ledger_entries is not None and max_ledger_entries > 0:
        ledger = ledger[-max_ledger_entries:]

    sources_seen = list(
        dict.fromkeys(list(old_record.get("sources_seen") or []) + [source])
    )
    merged["evidence_ledger"] = ledger
    merged["fact_provenance"] = provenance
    merged["retained_fields"] = retained_sorted
    merged["sources_seen"] = sources_seen
    merged["observations_count"] = int(old_record.get("observations_count") or 0) + 1
    merged["last_reconciled_at"] = ts
    return merged

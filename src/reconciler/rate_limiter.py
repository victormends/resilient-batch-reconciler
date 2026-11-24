"""Persisted SRE rate limiter with batch pacing, exponential backoff, and canary tripwires."""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional, Tuple, Union

from .state_store import AtomicJsonStore


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso_utc(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        raw = value[:-1] + "+00:00" if value.endswith("Z") else value
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def _to_iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class RateLimitConfig:
    """Policy thresholds for bounded SRE reconciliation and remediation loops."""

    daily_cap: int = 200
    session_cap: int = 50
    batch_size: int = 10
    batch_pause_min_sec: float = 30.0
    batch_pause_max_sec: float = 60.0
    inter_action_delay_min_sec: float = 0.0
    inter_action_delay_max_sec: float = 0.0
    backoff_base_sec: float = 2.0
    backoff_max_sec: float = 300.0
    canary_cooldown_sec: float = 14_400.0
    enforce_caps: bool = True
    canary_status_codes: Tuple[int, ...] = (429, 503)
    canary_endpoint_signals: Tuple[str, ...] = (
        "/circuit-open",
        "/maintenance-freeze",
        "/canary-halted",
    )
    canary_payload_signals: Tuple[str, ...] = (
        "rate limit exceeded",
        "too many requests",
        "throttlingexception",
        "circuit breaker open",
        "canary error budget exhausted",
        "maintenance freeze",
        "service unavailable",
    )


@dataclass(frozen=True)
class RateLimitDecision:
    """Outcome of a pre-action pacing and safety gate check."""

    allowed: bool
    reason: str
    delay_applied_sec: float = 0.0
    cooldown_remaining_sec: float = 0.0


class RateLimiter:
    """Persisted rate limiter enforcing daily/session budgets, backoff, and canary tripwires."""

    def __init__(
        self,
        state_path: Union[str, Path],
        config: Optional[RateLimitConfig] = None,
        *,
        clock: Optional[Callable[[], datetime]] = None,
        sleep_fn: Optional[Callable[[float], None]] = None,
        rng: Optional[random.Random] = None,
    ) -> None:
        self.config = config or RateLimitConfig()
        self.store = AtomicJsonStore(state_path)
        self._clock = clock or _utc_now
        self._sleep_fn = sleep_fn or time.sleep
        self._rng = rng or random.Random()
        self.session_count = 0
        self.consecutive_errors = 0
        self._ensure_initialized()

    def __enter__(self) -> "RateLimiter":
        return self

    def __exit__(self, *_: Any) -> None:
        return None

    async def __aenter__(self) -> "RateLimiter":
        return self

    async def __aexit__(self, *_: Any) -> None:
        return None

    def _today_str(self, now: datetime) -> str:
        return now.astimezone(timezone.utc).strftime("%Y-%m-%d")

    def _normalize_state(self, raw: Dict[str, Any], now: datetime) -> Dict[str, Any]:
        today = self._today_str(now)
        state = dict(raw)
        if state.get("date") != today:
            state["date"] = today
            state["daily_count"] = 0
            state["completed_keys"] = []
        state.setdefault("daily_count", 0)
        state.setdefault("completed_keys", [])
        state.setdefault("last_canary_tripwire_at", None)
        state.setdefault("last_canary_signal", None)
        state.setdefault("cooldown_until", None)
        state.setdefault("consecutive_errors", 0)
        return state

    def _ensure_initialized(self) -> None:
        now = self._clock()
        state = self.store.update(lambda raw: self._normalize_state(raw, now))
        self.consecutive_errors = int(state.get("consecutive_errors", 0))

    def is_in_cooldown(self) -> Tuple[bool, float]:
        """Return ``(active, remaining_seconds)`` for any persisted canary tripwire cooldown."""
        now = self._clock()
        state = self.store.load()
        until = _parse_iso_utc(state.get("cooldown_until"))
        if until is None:
            return False, 0.0
        remaining = (until - now).total_seconds()
        if remaining > 0:
            return True, remaining
        return False, 0.0

    def acquire(self) -> RateLimitDecision:
        """Check safety gates and apply pacing/batch pauses before executing an action."""
        now = self._clock()
        holder: Dict[str, Any] = {}

        def _check(raw: Dict[str, Any]) -> Dict[str, Any]:
            state = self._normalize_state(raw, now)
            until = _parse_iso_utc(state.get("cooldown_until"))
            if until is not None:
                rem = (until - now).total_seconds()
                if rem > 0 and self.config.enforce_caps:
                    holder["decision"] = RateLimitDecision(
                        allowed=False,
                        reason="canary_cooldown",
                        cooldown_remaining_sec=round(rem, 3),
                    )
                    return state

            if int(state["daily_count"]) >= self.config.daily_cap and self.config.enforce_caps:
                holder["decision"] = RateLimitDecision(allowed=False, reason="daily_cap")
                return state

            if self.session_count >= self.config.session_cap and self.config.enforce_caps:
                holder["decision"] = RateLimitDecision(allowed=False, reason="session_cap")
                return state

            holder["decision"] = None
            return state

        self.store.update(_check)
        if holder["decision"] is not None:
            return holder["decision"]

        delay = 0.0
        if (
            self.session_count > 0
            and self.config.batch_size > 0
            and self.session_count % self.config.batch_size == 0
        ):
            delay = self._rng.uniform(
                self.config.batch_pause_min_sec,
                self.config.batch_pause_max_sec,
            )
        elif self.config.inter_action_delay_max_sec > 0.0:
            delay = self._rng.uniform(
                self.config.inter_action_delay_min_sec,
                self.config.inter_action_delay_max_sec,
            )

        if delay > 0.0:
            self._sleep_fn(delay)

        return RateLimitDecision(allowed=True, reason="ok", delay_applied_sec=round(delay, 4))

    async def async_acquire(self) -> RateLimitDecision:
        """Async variant of ``acquire()`` delegating pacing sleeps to ``asyncio.sleep``."""
        now = self._clock()
        state = self.store.update(lambda raw: self._normalize_state(raw, now))
        until = _parse_iso_utc(state.get("cooldown_until"))
        if until is not None:
            rem = (until - now).total_seconds()
            if rem > 0 and self.config.enforce_caps:
                return RateLimitDecision(
                    allowed=False,
                    reason="canary_cooldown",
                    cooldown_remaining_sec=round(rem, 3),
                )
        if int(state["daily_count"]) >= self.config.daily_cap and self.config.enforce_caps:
            return RateLimitDecision(allowed=False, reason="daily_cap")
        if self.session_count >= self.config.session_cap and self.config.enforce_caps:
            return RateLimitDecision(allowed=False, reason="session_cap")

        delay = 0.0
        if (
            self.session_count > 0
            and self.config.batch_size > 0
            and self.session_count % self.config.batch_size == 0
        ):
            delay = self._rng.uniform(
                self.config.batch_pause_min_sec,
                self.config.batch_pause_max_sec,
            )
        elif self.config.inter_action_delay_max_sec > 0.0:
            delay = self._rng.uniform(
                self.config.inter_action_delay_min_sec,
                self.config.inter_action_delay_max_sec,
            )

        if delay > 0.0:
            await asyncio.sleep(delay)
        return RateLimitDecision(allowed=True, reason="ok", delay_applied_sec=round(delay, 4))

    def check_canary_signal(
        self,
        *,
        status_code: Optional[int] = None,
        endpoint: str = "",
        payload_text: str = "",
        headers: Optional[Mapping[str, str]] = None,
    ) -> Optional[str]:
        """Inspect a downstream control-plane response for canary tripwire indicators."""
        if status_code is not None and int(status_code) in self.config.canary_status_codes:
            return f"http_status:{int(status_code)}"

        if headers:
            lowered = {str(k).lower(): str(v).strip().lower() for k, v in headers.items()}
            if lowered.get("x-canary-tripwire") in ("1", "true", "tripped", "open"):
                return "header_signal:x-canary-tripwire"
            if lowered.get("x-circuit-breaker") in ("open", "tripped"):
                return "header_signal:x-circuit-breaker"

        endpoint_lower = endpoint.lower()
        for signal in self.config.canary_endpoint_signals:
            if signal.lower() in endpoint_lower:
                return f"endpoint_signal:{signal}"

        body_lower = payload_text.lower()
        for signal in self.config.canary_payload_signals:
            if signal.lower() in body_lower:
                return f"payload_signal:{signal}"

        return None

    def trigger_canary_cooldown(
        self,
        signal: str,
        *,
        cooldown_sec: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Persist a canary tripwire timestamp and cooldown expiration across restarts."""
        now = self._clock()
        duration = float(
            self.config.canary_cooldown_sec if cooldown_sec is None else cooldown_sec
        )
        until = now + timedelta(seconds=max(0.0, duration))

        def _trip(raw: Dict[str, Any]) -> Dict[str, Any]:
            state = self._normalize_state(raw, now)
            state["last_canary_tripwire_at"] = _to_iso_utc(now)
            state["last_canary_signal"] = signal
            state["cooldown_until"] = _to_iso_utc(until)
            return state

        return self.store.update(_trip)

    def clear_canary_cooldown(self) -> Dict[str, Any]:
        """Clear an active canary cooldown after manual verification."""
        now = self._clock()

        def _clear(raw: Dict[str, Any]) -> Dict[str, Any]:
            state = self._normalize_state(raw, now)
            state["cooldown_until"] = None
            return state

        return self.store.update(_clear)

    def record_success(self, asset_key: Optional[str] = None) -> Dict[str, Any]:
        """Reset error backoff and increment persisted daily and session budgets."""
        now = self._clock()
        self.consecutive_errors = 0
        self.session_count += 1

        def _success(raw: Dict[str, Any]) -> Dict[str, Any]:
            state = self._normalize_state(raw, now)
            state["consecutive_errors"] = 0
            state["daily_count"] = int(state["daily_count"]) + 1
            if asset_key and asset_key not in state["completed_keys"]:
                state["completed_keys"].append(asset_key)
            return state

        return self.store.update(_success)

    def record_error(self, reason: str = "") -> float:
        """Apply bounded exponential backoff after a transient execution failure."""
        now = self._clock()
        holder: Dict[str, int] = {}

        def _err(raw: Dict[str, Any]) -> Dict[str, Any]:
            state = self._normalize_state(raw, now)
            err_count = int(state.get("consecutive_errors", 0)) + 1
            state["consecutive_errors"] = err_count
            state["last_error_reason"] = reason
            state["last_error_at"] = _to_iso_utc(now)
            holder["count"] = err_count
            return state

        self.store.update(_err)
        self.consecutive_errors = holder["count"]
        delay = min(
            self.config.backoff_base_sec * (2 ** (self.consecutive_errors - 1)),
            self.config.backoff_max_sec,
        )
        if delay > 0.0:
            self._sleep_fn(delay)
        return float(delay)

    def is_completed_today(self, asset_key: str) -> bool:
        now = self._clock()
        state = self._normalize_state(self.store.load(), now)
        return asset_key in state["completed_keys"]

    def stats(self) -> Dict[str, Any]:
        now = self._clock()
        state = self._normalize_state(self.store.load(), now)
        cooldown_active, cooldown_remaining = self.is_in_cooldown()
        return {
            "date": state["date"],
            "session_count": self.session_count,
            "session_remaining": max(0, self.config.session_cap - self.session_count),
            "daily_count": int(state["daily_count"]),
            "daily_remaining": max(0, self.config.daily_cap - int(state["daily_count"])),
            "consecutive_errors": int(state["consecutive_errors"]),
            "cooldown_active": cooldown_active,
            "cooldown_remaining_sec": round(cooldown_remaining, 3),
            "last_canary_signal": state.get("last_canary_signal"),
        }

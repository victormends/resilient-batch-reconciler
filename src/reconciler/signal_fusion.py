"""Inverse-variance Bayesian signal fusion for heterogeneous telemetry probes."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union


@dataclass(frozen=True)
class ProbeReading:
    """Single sensor/probe observation with calibrated measurement uncertainty."""

    source: str
    value: Optional[float]
    sigma: float
    role: str = "secondary"
    metric: Optional[str] = None
    bias_offset: float = 0.0
    enabled: bool = True

    def calibrated_value(self) -> float:
        if self.value is None:
            raise ValueError(f"ProbeReading {self.source!r} has no value")
        return float(self.value) + float(self.bias_offset)


@dataclass(frozen=True)
class FusedSignal:
    """Result of inverse-variance telemetry fusion with conflict diagnostics."""

    fused_value: float
    fused_sigma: float
    ci_low: float
    ci_high: float
    confidence: int
    primary_source: str
    conflict_flag: bool
    conflict_delta: float
    readings_count: int
    weights: Dict[str, float] = field(default_factory=dict)
    effective_sigmas: Dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "fused_value": self.fused_value,
            "fused_sigma": self.fused_sigma,
            "ci_low": self.ci_low,
            "ci_high": self.ci_high,
            "confidence": self.confidence,
            "primary_source": self.primary_source,
            "conflict_flag": self.conflict_flag,
            "conflict_delta": self.conflict_delta,
            "readings_count": self.readings_count,
            "weights": dict(self.weights),
            "effective_sigmas": dict(self.effective_sigmas),
        }


def _coerce_reading(item: Union[ProbeReading, Mapping[str, Any]]) -> ProbeReading:
    if isinstance(item, ProbeReading):
        return item
    if isinstance(item, Mapping):
        return ProbeReading(
            source=str(item["source"]),
            value=item.get("value"),
            sigma=float(item["sigma"]),
            role=str(item.get("role", "secondary")),
            metric=item.get("metric"),
            bias_offset=float(item.get("bias_offset", 0.0)),
            enabled=bool(item.get("enabled", True)),
        )
    raise TypeError(f"Unsupported reading type: {type(item)!r}")


def _clamp(val: float, low: float, high: float) -> float:
    return max(low, min(high, val))


def fuse_probe_readings(
    readings: Sequence[Union[ProbeReading, Mapping[str, Any]]],
    conflict_threshold: float,
    z_score: float = 1.28,
    *,
    conflict_inflation_factor: float = 5.0,
    min_bound: Optional[float] = None,
    max_bound: Optional[float] = None,
    reference_sigma_scale: Optional[float] = None,
    round_digits: int = 4,
) -> Optional[FusedSignal]:
    """Fuse heterogeneous probe readings using inverse-variance weighting (w_i = 1 / sigma_i^2).

    Conflict handling:
    - Selects the primary anchor probe (lowest-sigma probe marked ``role='primary'``,
      or the lowest-sigma probe overall when none is marked primary).
    - Computes absolute divergence against secondary probes. If any divergence meets or
      exceeds ``conflict_threshold``, ``conflict_flag`` is raised and variance inflation
      is applied so overconfident disagreeing sensors cannot silently dominate the estimate.
    """
    if conflict_threshold < 0:
        raise ValueError("conflict_threshold must be non-negative")
    if z_score <= 0:
        raise ValueError("z_score must be positive")
    if conflict_inflation_factor < 1.0:
        raise ValueError("conflict_inflation_factor must be >= 1.0")
    if min_bound is not None and max_bound is not None and min_bound > max_bound:
        raise ValueError("min_bound must be <= max_bound")

    parsed: List[ProbeReading] = []
    seen_metrics = set()
    for raw in readings:
        r = _coerce_reading(raw)
        if not r.enabled or r.value is None:
            continue
        if not r.source or not r.source.strip():
            raise ValueError("ProbeReading source must be non-empty")
        val = float(r.value)
        sig = float(r.sigma)
        if not math.isfinite(val) or not math.isfinite(sig) or sig <= 0.0:
            raise ValueError(f"Invalid finite value/sigma on probe {r.source!r}: value={r.value}, sigma={r.sigma}")
        if r.role not in ("primary", "secondary"):
            raise ValueError(f"ProbeReading role must be 'primary' or 'secondary', got {r.role!r}")
        if r.metric:
            seen_metrics.add(r.metric)
        parsed.append(r)

    if len(seen_metrics) > 1:
        raise ValueError(f"Cannot fuse heterogeneous metrics in one call: {sorted(seen_metrics)}")

    if not parsed:
        return None

    # Deduplicate source labels deterministically if duplicates appear
    labeled: List[Tuple[str, ProbeReading]] = []
    label_counts: Dict[str, int] = {}
    for r in parsed:
        cnt = label_counts.get(r.source, 0) + 1
        label_counts[r.source] = cnt
        label = r.source if cnt == 1 else f"{r.source}#{cnt}"
        labeled.append((label, r))

    primary_candidates = [item for item in labeled if item[1].role == "primary"]
    anchor_label, anchor_reading = min(
        primary_candidates or labeled,
        key=lambda item: float(item[1].sigma),
    )

    anchor_val = anchor_reading.calibrated_value()
    secondaries = [(lbl, rd) for lbl, rd in labeled if lbl != anchor_label]

    conflicting_secs: List[Tuple[str, ProbeReading, float]] = []
    agreeing_secs: List[Tuple[str, ProbeReading, float]] = []
    max_delta = 0.0

    for lbl, rd in secondaries:
        delta = abs(anchor_val - rd.calibrated_value())
        if delta > max_delta:
            max_delta = delta
        if delta >= conflict_threshold:
            conflicting_secs.append((lbl, rd, delta))
        else:
            agreeing_secs.append((lbl, rd, delta))

    conflict_flag = len(conflicting_secs) > 0
    effective_sigmas: Dict[str, float] = {lbl: float(rd.sigma) for lbl, rd in labeled}

    if conflict_flag:
        if len(conflicting_secs) > len(agreeing_secs):
            min_conflict_sec_sigma = min(float(rd.sigma) for _, rd, _ in conflicting_secs)
            effective_sigmas[anchor_label] = float(anchor_reading.sigma) * conflict_inflation_factor
            if float(anchor_reading.sigma) >= min_conflict_sec_sigma:
                for lbl, rd, _ in conflicting_secs:
                    effective_sigmas[lbl] = float(rd.sigma) * conflict_inflation_factor
        else:
            for lbl, rd, _ in conflicting_secs:
                effective_sigmas[lbl] = float(rd.sigma) * conflict_inflation_factor

    raw_weights: Dict[str, float] = {
        lbl: 1.0 / (effective_sigmas[lbl] ** 2) for lbl, _ in labeled
    }
    w_sum = sum(raw_weights.values())
    fused_val = sum(raw_weights[lbl] * rd.calibrated_value() for lbl, rd in labeled) / w_sum
    fused_sigma = math.sqrt(1.0 / w_sum)

    ci_low_raw = fused_val - z_score * fused_sigma
    ci_high_raw = fused_val + z_score * fused_sigma
    if min_bound is not None:
        ci_low_raw = max(min_bound, ci_low_raw)
        fused_val = max(min_bound, fused_val)
    if max_bound is not None:
        ci_high_raw = min(max_bound, ci_high_raw)
        fused_val = min(max_bound, fused_val)

    max_nominal_sigma = max(float(rd.sigma) for _, rd in labeled)
    if reference_sigma_scale is not None:
        if reference_sigma_scale <= 0:
            raise ValueError("reference_sigma_scale must be positive")
        scale = float(reference_sigma_scale)
    else:
        floor = 1.0 if max_nominal_sigma >= 0.5 else 0.25
        scale = max(abs(fused_val) * 0.20, floor)

    raw_conf = 100.0 * (1.0 / (1.0 + (fused_sigma / scale)))
    if conflict_flag:
        raw_conf *= 0.85
    confidence = int(round(_clamp(raw_conf, 1.0, 99.0)))

    primary_source = max(raw_weights.items(), key=lambda kv: kv[1])[0]
    normalized_weights = {
        lbl: round(w / w_sum, round_digits) for lbl, w in raw_weights.items()
    }
    rounded_sigmas = {lbl: round(sig, round_digits) for lbl, sig in effective_sigmas.items()}

    return FusedSignal(
        fused_value=round(fused_val, round_digits),
        fused_sigma=round(fused_sigma, round_digits),
        ci_low=round(ci_low_raw, round_digits),
        ci_high=round(ci_high_raw, round_digits),
        confidence=confidence,
        primary_source=primary_source,
        conflict_flag=conflict_flag,
        conflict_delta=round(max_delta, round_digits),
        readings_count=len(labeled),
        weights=normalized_weights,
        effective_sigmas=rounded_sigmas,
    )

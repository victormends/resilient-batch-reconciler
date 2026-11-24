"""Tests for inverse-variance probe telemetry fusion and conflict variance inflation."""

from __future__ import annotations

import math

import pytest

from reconciler.signal_fusion import ProbeReading, fuse_probe_readings


def test_inverse_variance_fusion_weights_and_confidence_interval() -> None:
    readings = [
        ProbeReading(source="ebpf_kernel", value=20.0, sigma=1.0, role="primary", metric="latency_ms"),
        ProbeReading(source="envoy_sidecar", value=23.0, sigma=2.0, role="secondary", metric="latency_ms"),
    ]
    fused = fuse_probe_readings(readings, conflict_threshold=10.0, z_score=1.28)
    assert fused is not None
    assert fused.conflict_flag is False
    # w1 = 1.0, w2 = 0.25 -> (20*1 + 23*0.25) / 1.25 = 20.6
    assert fused.fused_value == pytest.approx(20.6, abs=1e-4)
    expected_sigma = math.sqrt(1.0 / 1.25)
    assert fused.fused_sigma == pytest.approx(expected_sigma, abs=1e-4)
    assert fused.ci_low == pytest.approx(20.6 - 1.28 * expected_sigma, abs=1e-3)
    assert fused.ci_high == pytest.approx(20.6 + 1.28 * expected_sigma, abs=1e-3)
    assert fused.primary_source == "ebpf_kernel"
    assert fused.weights["ebpf_kernel"] == pytest.approx(0.8, abs=1e-4)
    assert fused.weights["envoy_sidecar"] == pytest.approx(0.2, abs=1e-4)


def test_conflict_inflates_overconfident_primary_variance_and_lowers_confidence() -> None:
    agreeing = fuse_probe_readings(
        [
            {"source": "synthetic_probe", "value": 100.0, "sigma": 0.5, "role": "primary"},
            {"source": "node_exporter", "value": 101.0, "sigma": 2.5, "role": "secondary"},
        ],
        conflict_threshold=15.0,
    )
    conflicting = fuse_probe_readings(
        [
            {"source": "synthetic_probe", "value": 100.0, "sigma": 0.5, "role": "primary"},
            {"source": "node_exporter", "value": 140.0, "sigma": 2.5, "role": "secondary"},
        ],
        conflict_threshold=15.0,
        conflict_inflation_factor=5.0,
    )
    assert agreeing is not None and conflicting is not None
    assert agreeing.conflict_flag is False
    assert conflicting.conflict_flag is True
    assert conflicting.conflict_delta == pytest.approx(40.0, abs=1e-4)
    assert conflicting.effective_sigmas["synthetic_probe"] == pytest.approx(2.5, abs=1e-4)
    # After inflation 0.5 -> 2.5, both probes have equal weight (0.5 / 0.5) -> midpoint 120.0
    assert conflicting.fused_value == pytest.approx(120.0, abs=1e-4)
    assert conflicting.fused_sigma > agreeing.fused_sigma
    assert conflicting.confidence < agreeing.confidence


def test_three_probe_outlier_suppression_vs_majority_override() -> None:
    # Two agreeing probes vs one outlier secondary -> outlier gets inflated
    outlier_case = fuse_probe_readings(
        [
            ProbeReading("p1", 50.0, sigma=1.0, role="primary"),
            ProbeReading("s1", 51.0, sigma=1.5, role="secondary"),
            ProbeReading("s2_outlier", 120.0, sigma=1.5, role="secondary"),
        ],
        conflict_threshold=15.0,
    )
    assert outlier_case is not None
    assert outlier_case.conflict_flag is True
    assert outlier_case.effective_sigmas["s2_outlier"] > 1.5
    assert outlier_case.effective_sigmas["p1"] == pytest.approx(1.0)
    assert outlier_case.fused_value < 55.0

    # Primary contradicted by two agreeing secondaries -> primary gets inflated
    primary_bad = fuse_probe_readings(
        [
            ProbeReading("p1_stale", 120.0, sigma=0.5, role="primary"),
            ProbeReading("s1", 50.0, sigma=1.5, role="secondary"),
            ProbeReading("s2", 51.0, sigma=1.5, role="secondary"),
        ],
        conflict_threshold=15.0,
    )
    assert primary_bad is not None
    assert primary_bad.conflict_flag is True
    assert primary_bad.effective_sigmas["p1_stale"] == pytest.approx(2.5)
    assert primary_bad.fused_value < 70.0


def test_bias_offset_bounds_clamping_and_validation() -> None:
    fused = fuse_probe_readings(
        [
            ProbeReading("cap_a", 96.0, sigma=2.0, bias_offset=2.0, role="primary"),
            ProbeReading("cap_b", 99.0, sigma=2.0, role="secondary"),
        ],
        conflict_threshold=10.0,
        min_bound=0.0,
        max_bound=100.0,
    )
    assert fused is not None
    assert fused.fused_value == pytest.approx(98.5, abs=1e-4)
    assert fused.ci_high <= 100.0
    assert fused.ci_low >= 0.0

    assert fuse_probe_readings([], conflict_threshold=5.0) is None
    assert fuse_probe_readings([ProbeReading("disabled", 10.0, 1.0, enabled=False)], 5.0) is None

    with pytest.raises(ValueError):
        fuse_probe_readings([ProbeReading("bad_sigma", 10.0, sigma=0.0)], conflict_threshold=5.0)
    with pytest.raises(ValueError):
        fuse_probe_readings(
            [
                ProbeReading("m1", 10.0, sigma=1.0, metric="latency_ms"),
                ProbeReading("m2", 50.0, sigma=1.0, metric="capacity_pct"),
            ],
            conflict_threshold=5.0,
        )

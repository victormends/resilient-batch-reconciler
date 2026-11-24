"""Tests for 2x2 Pillow synthetic dashboard snapshot grid batching and reverse mapping."""

from __future__ import annotations

import base64
import io
from pathlib import Path
from typing import Tuple

import pytest
from PIL import Image, ImageDraw

from reconciler.grid_batcher import (
    GRID_CELL_LABELS,
    SnapshotInput,
    batch_compose_snapshot_grids,
    compose_snapshot_grid,
)


def _make_synthetic_dashboard(
    fill_rgb: Tuple[int, int, int],
    size: Tuple[int, int] = (320, 200),
) -> Image.Image:
    """Generate a synthetic SRE telemetry bar-chart image with no external assets."""
    img = Image.new("RGB", size, (15, 20, 26))
    draw = ImageDraw.Draw(img)
    draw.rectangle([10, 10, size[0] - 11, size[1] - 11], outline=fill_rgb, width=3)
    for bar_idx in range(5):
        x0 = 28 + bar_idx * 52
        bar_h = 40 + (bar_idx * 22) % 110
        draw.rectangle([x0, size[1] - 25 - bar_h, x0 + 30, size[1] - 25], fill=fill_rgb)
    return img


def test_compose_2x2_grid_labels_and_reverse_mappings(tmp_path: Path) -> None:
    dash1_path = tmp_path / "dash_checkout.png"
    _make_synthetic_dashboard((0, 200, 120)).save(dash1_path)

    snapshots = [
        SnapshotInput("dash-checkout-latency", dash1_path),
        ("dash-auth-cpu", _make_synthetic_dashboard((230, 160, 20))),
        {"dashboard_id": "dash-kafka-lag", "image": _make_synthetic_dashboard((220, 60, 60))},
        SnapshotInput("dash-redis-hit", _make_synthetic_dashboard((60, 140, 240))),
    ]

    result = compose_snapshot_grid(snapshots, cell_width=300, cell_height=220, padding=10)
    expected_w = 2 * 300 + 3 * 10
    expected_h = 2 * 220 + 3 * 10
    assert result.grid_size == (expected_w, expected_h)
    assert result.image.size == (expected_w, expected_h)

    assert tuple(result.cell_to_dashboard.keys()) == GRID_CELL_LABELS
    assert result.resolve_label("a1") == "dash-checkout-latency"
    assert result.resolve_label("B1") == "dash-auth-cpu"
    assert result.resolve_label("A2") == "dash-kafka-lag"
    assert result.resolve_label("b2") == "dash-redis-hit"
    assert result.dashboard_to_cell["dash-kafka-lag"] == "A2"

    # Coordinate reverse lookup checks center of each quadrant
    for label, (x0, y0, x1, y1) in result.cell_boxes.items():
        cx = (x0 + x1) // 2
        cy = (y0 + y1) // 2
        assert result.resolve_coordinates(cx, cy) == result.cell_to_dashboard[label]
    assert result.resolve_coordinates(0, 0) is None  # inside outer padding

    # Reverse-map batched triage findings back to dashboard IDs
    mapped = result.map_findings(
        {
            "A1": {"status": "healthy"},
            "A2": {"status": "spike_detected", "p99_ms": 480},
        }
    )
    assert mapped == {
        "dash-checkout-latency": {"status": "healthy"},
        "dash-kafka-lag": {"status": "spike_detected", "p99_ms": 480},
    }

    # Verify serialization helpers produce valid PNG bytes and save cleanly
    b64 = result.to_base64("PNG")
    reopened = Image.open(io.BytesIO(base64.b64decode(b64)))
    assert reopened.size == (expected_w, expected_h)

    out_file = result.save(tmp_path / "out" / "grid.png")
    assert out_file.exists()


def test_partial_grid_and_batch_chunking_4x_reduction() -> None:
    dashboards = [
        SnapshotInput(f"dash-svc-{idx:02d}", _make_synthetic_dashboard((40 + idx * 20, 120, 200)))
        for idx in range(9)
    ]
    grids = batch_compose_snapshot_grids(dashboards, cell_width=240, cell_height=180)
    assert len(grids) == 3  # 9 dashboards packed into 3 grids (4 + 4 + 1)

    assert len(grids[0].cell_to_dashboard) == 4
    assert len(grids[1].cell_to_dashboard) == 4
    assert len(grids[2].cell_to_dashboard) == 1
    assert grids[2].resolve_label("A1") == "dash-svc-08"
    assert grids[2].resolve_label("B2") is None

    reconstructed_ids = [
        did for grid in grids for did in grid.cell_to_dashboard.values()
    ]
    assert reconstructed_ids == [f"dash-svc-{idx:02d}" for idx in range(9)]


def test_grid_batcher_input_validation() -> None:
    with pytest.raises(ValueError):
        compose_snapshot_grid([])

    five = [SnapshotInput(f"d{i}", _make_synthetic_dashboard((10, 20, 30))) for i in range(5)]
    with pytest.raises(ValueError):
        compose_snapshot_grid(five)

    dups = [
        SnapshotInput("same-dash", _make_synthetic_dashboard((10, 20, 30))),
        SnapshotInput("same-dash", _make_synthetic_dashboard((40, 50, 60))),
    ]
    with pytest.raises(ValueError, match="Duplicate dashboard_id"):
        compose_snapshot_grid(dups)

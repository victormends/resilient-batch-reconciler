"""2x2 Pillow dashboard snapshot grid compositor for 4x cheaper visual anomaly triage."""

from __future__ import annotations

import base64
import io
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from PIL import Image, ImageDraw, ImageFont, ImageOps

GRID_CELL_LABELS: Tuple[str, ...] = ("A1", "B1", "A2", "B2")


@dataclass(frozen=True)
class SnapshotInput:
    """Input descriptor pairing a synthetic dashboard ID with an image path or PIL Image."""

    dashboard_id: str
    image: Union[str, Path, Image.Image]


@dataclass
class SnapshotGridResult:
    """Composed 2x2 dashboard snapshot grid with bidirectional label and coordinate mappings."""

    image: Image.Image
    cell_to_dashboard: Dict[str, str]
    dashboard_to_cell: Dict[str, str]
    cell_boxes: Dict[str, Tuple[int, int, int, int]]
    grid_size: Tuple[int, int]

    def resolve_label(self, cell_label: str) -> Optional[str]:
        """Resolve a cell label (e.g. 'A1', 'b2') back to its dashboard ID."""
        if not cell_label:
            return None
        return self.cell_to_dashboard.get(cell_label.strip().upper())

    def resolve_coordinates(self, x: int, y: int) -> Optional[str]:
        """Resolve a pixel coordinate inside the composite grid back to a dashboard ID."""
        for label, (x0, y0, x1, y1) in self.cell_boxes.items():
            if x0 <= x < x1 and y0 <= y < y1:
                return self.cell_to_dashboard.get(label)
        return None

    def map_findings(self, findings_by_cell: Mapping[str, Any]) -> Dict[str, Any]:
        """Translate cell-keyed triage outputs (e.g. {'A1': {...}}) to dashboard IDs."""
        mapped: Dict[str, Any] = {}
        for cell_label, finding in findings_by_cell.items():
            dashboard_id = self.resolve_label(cell_label)
            if dashboard_id is not None:
                mapped[dashboard_id] = finding
        return mapped

    def to_bytes(self, image_format: str = "PNG", **save_kwargs: Any) -> bytes:
        buf = io.BytesIO()
        self.image.save(buf, format=image_format, **save_kwargs)
        return buf.getvalue()

    def to_base64(self, image_format: str = "PNG", **save_kwargs: Any) -> str:
        raw = self.to_bytes(image_format=image_format, **save_kwargs)
        return base64.b64encode(raw).decode("ascii")

    def save(self, path: Union[str, Path], image_format: Optional[str] = None) -> Path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        if image_format:
            self.image.save(target, format=image_format)
        else:
            self.image.save(target)
        return target


def _coerce_snapshot(
    item: Union[SnapshotInput, Mapping[str, Any], Tuple[str, Union[str, Path, Image.Image]]],
) -> SnapshotInput:
    if isinstance(item, SnapshotInput):
        snap = item
    elif isinstance(item, Mapping):
        snap = SnapshotInput(
            dashboard_id=str(item["dashboard_id"]),
            image=item["image"],
        )
    elif isinstance(item, tuple) and len(item) == 2:
        snap = SnapshotInput(dashboard_id=str(item[0]), image=item[1])
    else:
        raise TypeError(f"Unsupported snapshot entry: {type(item)!r}")

    if not snap.dashboard_id.strip():
        raise ValueError("dashboard_id must be a non-empty string")
    return snap


def _load_rgb_image(source: Union[str, Path, Image.Image]) -> Image.Image:
    if isinstance(source, Image.Image):
        return ImageOps.exif_transpose(source).convert("RGB")
    img_path = Path(source)
    with Image.open(img_path) as opened:
        return ImageOps.exif_transpose(opened).convert("RGB")


def compose_snapshot_grid(
    snapshots: Sequence[
        Union[SnapshotInput, Mapping[str, Any], Tuple[str, Union[str, Path, Image.Image]]]
    ],
    *,
    cell_width: int = 400,
    cell_height: int = 300,
    header_height: int = 28,
    padding: int = 8,
    background_color: Tuple[int, int, int] = (18, 22, 28),
    cell_bg_color: Tuple[int, int, int] = (24, 30, 38),
    border_color: Tuple[int, int, int] = (56, 68, 84),
    badge_bg_color: Tuple[int, int, int] = (30, 136, 229),
    text_color: Tuple[int, int, int] = (240, 244, 248),
) -> SnapshotGridResult:
    """Pack 1 to 4 synthetic dashboard snapshots into a labeled 2x2 grid image (A1..B2)."""
    if not snapshots:
        raise ValueError("snapshots must contain at least 1 item")
    if len(snapshots) > 4:
        raise ValueError("compose_snapshot_grid accepts at most 4 snapshots per 2x2 grid")
    if cell_width <= 64 or cell_height <= 64:
        raise ValueError("cell_width and cell_height must exceed 64 pixels")
    if header_height < 16 or header_height >= cell_height - 16:
        raise ValueError("header_height must fit within cell_height")

    items = [_coerce_snapshot(entry) for entry in snapshots]
    seen_ids = set()
    for item in items:
        if item.dashboard_id in seen_ids:
            raise ValueError(f"Duplicate dashboard_id in grid batch: {item.dashboard_id!r}")
        seen_ids.add(item.dashboard_id)

    cols, rows = 2, 2
    total_w = cols * cell_width + (cols + 1) * padding
    total_h = rows * cell_height + (rows + 1) * padding
    grid_image = Image.new("RGB", (total_w, total_h), background_color)
    draw = ImageDraw.Draw(grid_image)
    font = ImageFont.load_default()

    cell_to_dashboard: Dict[str, str] = {}
    dashboard_to_cell: Dict[str, str] = {}
    cell_boxes: Dict[str, Tuple[int, int, int, int]] = {}

    for idx, label in enumerate(GRID_CELL_LABELS):
        col = idx % cols
        row = idx // cols
        x0 = padding + col * (cell_width + padding)
        y0 = padding + row * (cell_height + padding)
        x1 = x0 + cell_width
        y1 = y0 + cell_height
        cell_boxes[label] = (x0, y0, x1, y1)

        draw.rectangle([x0, y0, x1 - 1, y1 - 1], fill=cell_bg_color, outline=border_color, width=1)
        draw.rectangle([x0, y0, x1 - 1, y0 + header_height - 1], fill=(32, 40, 52))
        draw.rectangle([x0 + 4, y0 + 4, x0 + 36, y0 + header_height - 5], fill=badge_bg_color)
        draw.text((x0 + 12, y0 + 8), label, fill=text_color, font=font)

        if idx >= len(items):
            draw.text((x0 + 44, y0 + 8), "EMPTY", fill=(130, 142, 158), font=font)
            continue

        snap = items[idx]
        cell_to_dashboard[label] = snap.dashboard_id
        dashboard_to_cell[snap.dashboard_id] = label

        max_chars = max(8, (cell_width - 54) // 7)
        title = snap.dashboard_id if len(snap.dashboard_id) <= max_chars else snap.dashboard_id[: max_chars - 3] + "..."
        draw.text((x0 + 44, y0 + 8), title, fill=text_color, font=font)

        rgb = _load_rgb_image(snap.image)
        avail_w = cell_width - 8
        avail_h = cell_height - header_height - 8
        fitted = ImageOps.contain(rgb, (avail_w, avail_h))
        paste_x = x0 + (cell_width - fitted.width) // 2
        paste_y = y0 + header_height + (cell_height - header_height - fitted.height) // 2
        grid_image.paste(fitted, (paste_x, paste_y))

    return SnapshotGridResult(
        image=grid_image,
        cell_to_dashboard=cell_to_dashboard,
        dashboard_to_cell=dashboard_to_cell,
        cell_boxes=cell_boxes,
        grid_size=(total_w, total_h),
    )


def batch_compose_snapshot_grids(
    snapshots: Sequence[
        Union[SnapshotInput, Mapping[str, Any], Tuple[str, Union[str, Path, Image.Image]]]
    ],
    **grid_kwargs: Any,
) -> List[SnapshotGridResult]:
    """Chunk an arbitrary sequence of dashboard snapshots into batches of 4 (2x2 grids)."""
    if not snapshots:
        return []
    grids: List[SnapshotGridResult] = []
    for start in range(0, len(snapshots), 4):
        chunk = snapshots[start : start + 4]
        grids.append(compose_snapshot_grid(chunk, **grid_kwargs))
    return grids

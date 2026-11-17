"""Tile completion markers for kill/resume (no Ray state)."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class TileMarker:
    campaign: str
    order: int
    pixel: int
    rows: int
    bytes: int
    sha256: str
    status: str = "done"

    @property
    def name(self) -> str:
        return f"o{self.order}_p{self.pixel}.json"


def marker_path(markers_dir: Path, order: int, pixel: int) -> Path:
    return markers_dir / f"o{order}_p{pixel}.json"


def write_marker(markers_dir: Path, marker: TileMarker) -> Path:
    markers_dir.mkdir(parents=True, exist_ok=True)
    path = marker_path(markers_dir, marker.order, marker.pixel)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(asdict(marker), indent=2, sort_keys=True) + "\n")
    tmp.replace(path)
    return path


def read_marker(path: Path) -> TileMarker:
    data = json.loads(path.read_text())
    return TileMarker(**data)


def file_sha256(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            block = fh.read(chunk)
            if not block:
                break
            h.update(block)
    return h.hexdigest()


def missing_tiles(markers_dir: Path, order: int, pixels: list[int]) -> list[int]:
    missing: list[int] = []
    for pixel in pixels:
        path = marker_path(markers_dir, order, pixel)
        if not path.is_file():
            missing.append(pixel)
            continue
        try:
            marker = read_marker(path)
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            missing.append(pixel)
            continue
        if marker.status != "done" or marker.sha256 == "":
            missing.append(pixel)
    return missing

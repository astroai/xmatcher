"""HEALPix tile geometry helpers for pensus campaigns."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TileSpec:
    """One HEALPix tile with core + margin in nested numbering."""

    order: int
    pixel: int
    margin_arcsec: float

    @property
    def nside(self) -> int:
        return 2**self.order


def margin_arcsec(
    *,
    r_max_arcsec: float,
    cascade_levels: int,
    pm_halo_arcsec: float,
) -> float:
    """Locked margin rule: 2 · r_max · L + PM halo."""
    if r_max_arcsec <= 0 or cascade_levels < 1 or pm_halo_arcsec < 0:
        raise ValueError("invalid margin inputs")
    return 2.0 * float(r_max_arcsec) * int(cascade_levels) + float(pm_halo_arcsec)


def tile_ids(order: int) -> range:
    """Inclusive nested pixel id range for a full-sky order."""
    if order < 0:
        raise ValueError("order must be >= 0")
    return range(12 * (4**order))


def approx_tile_area_deg2(order: int) -> float:
    return 41252.96125 / (12 * (4**order))


def choose_order_for_area(target_deg2: float) -> int:
    """Smallest order whose mean tile area is <= target_deg2."""
    if target_deg2 <= 0:
        raise ValueError("target_deg2 must be positive")
    order = 0
    while approx_tile_area_deg2(order) > target_deg2 and order < 12:
        order += 1
    return order


def chord_margin_deg(margin_arcsec_value: float) -> float:
    return float(margin_arcsec_value) / 3600.0


def assert_margin_covers_cascade(
    *,
    margin: float,
    r_max_arcsec: float,
    cascade_levels: int,
    pm_halo_arcsec: float,
    tol: float = 1e-9,
) -> None:
    need = margin_arcsec(
        r_max_arcsec=r_max_arcsec,
        cascade_levels=cascade_levels,
        pm_halo_arcsec=pm_halo_arcsec,
    )
    if margin + tol < need:
        raise AssertionError(f"margin {margin} < required {need}")

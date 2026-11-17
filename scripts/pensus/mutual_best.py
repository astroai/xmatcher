"""Tile-local mutual-best resolution for spine×survey edges."""

from __future__ import annotations

import polars as pl


def mutual_best_edges(
    edges: pl.DataFrame,
    *,
    left_id: str = "spine_id",
    right_id: str = "survey_id",
    sep: str = "sep_arcsec",
) -> pl.DataFrame:
    """Keep pairs that are mutual nearest neighbours by separation.

    Ties break by `(left_id, right_id)` lexicographic order for determinism.
    """
    if edges.is_empty():
        return edges
    required = {left_id, right_id, sep}
    missing = required - set(edges.columns)
    if missing:
        raise ValueError(f"mutual_best_edges missing columns: {sorted(missing)}")

    ranked = edges.sort([sep, left_id, right_id])
    best_for_left = ranked.unique(subset=[left_id], keep="first")
    best_for_right = ranked.unique(subset=[right_id], keep="first")
    keys = [left_id, right_id]
    return best_for_left.join(best_for_right.select(keys), on=keys, how="inner")

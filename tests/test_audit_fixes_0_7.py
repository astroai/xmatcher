"""Regressions for defects found in the 0.7.0 documentation audit."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from xmatcher import CrossMatch
from xmatcher import stilts as stilts_mod
from xmatcher.convert import convert
from xmatcher.io_utils import write_frame


@pytest.fixture()
def three_catalogues(tmp_path: Path) -> list[Path]:
    a = tmp_path / "a.parquet"
    b = tmp_path / "b.parquet"
    c = tmp_path / "c.parquet"
    pl.DataFrame({"id": [1, 2], "ra": [10.0, 20.0], "dec": [0.0, 1.0]}).write_parquet(a)
    # Source 8 is only in catalogue 2; source 10 only in catalogue 3.
    pl.DataFrame({"bid": [7, 8], "ra": [10.0001, 50.0], "dec": [0.0, 5.0]}).write_parquet(b)
    pl.DataFrame(
        {"cid": [9, 10], "RA_ICRS": [20.0001, 80.0], "DE_ICRS": [1.0, -5.0]}
    ).write_parquet(c)
    return [a, b, c]


def _read_hats(tree: Path, tmp_path: Path) -> pl.DataFrame:
    flat = tmp_path / f"{tree.stem}.parquet"
    convert(tree, flat, overwrite=True)
    return pl.read_parquet(flat)


def test_pairwise_union_writes_hats_with_second_catalogue_rows(
    three_catalogues: list[Path], tmp_path: Path
) -> None:
    pytest.importorskip("cdshealpix")
    a, b, _ = three_catalogues
    out = tmp_path / "u2.hats"
    CrossMatch().crossmatch(str(a), str(b), radius_arcsec=1.0, join_type="1or2", output_file=out)
    back = _read_hats(out, tmp_path).sort("_union_ra")
    assert back.height == 3
    assert back["bid"].to_list() == [7, None, 8]
    # A catalogue-2-only row is partitioned on catalogue 2's position.
    assert back["_union_ra"].to_list() == [10.0, 20.0, 50.0]
    assert back["_union_dec"].to_list() == [0.0, 1.0, 5.0]


def test_multi_catalogue_union_writes_hats_with_every_row(
    three_catalogues: list[Path], tmp_path: Path
) -> None:
    pytest.importorskip("cdshealpix")
    out = tmp_path / "u3.hats"
    CrossMatch().crossmatch_multi(
        [str(p) for p in three_catalogues],
        radius_arcsec=1.0,
        join_type="1or2",
        output_file=out,
    )
    back = _read_hats(out, tmp_path).sort("_union_ra")
    assert back.height == 4
    assert back["_union_ra"].to_list() == [10.0, 20.0, 50.0, 80.0]
    assert back["_union_dec"].to_list() == [0.0, 1.0, 5.0, -5.0]
    assert back["cid"].to_list() == [None, 9, None, 10]


def test_union_position_takes_ra_and_dec_from_the_same_catalogue(tmp_path: Path) -> None:
    pytest.importorskip("cdshealpix")
    # Row 2 has catalogue 1's RA but not its Dec: both must come from catalogue 2.
    frame = pl.DataFrame(
        {
            "ra": [10.0, 20.0],
            "dec": [0.0, None],
            "ra_2": [10.0001, 30.0],
            "dec_2": [0.0, 3.0],
        }
    )
    out = tmp_path / "mixed.hats"
    write_frame(frame, out, fallback_positions=[("ra_2", "dec_2")])
    back = _read_hats(out, tmp_path).sort("_union_ra")
    assert back.select("_union_ra", "_union_dec").rows() == [(10.0, 0.0), (30.0, 3.0)]


def test_hats_output_without_fallback_keeps_its_columns(tmp_path: Path) -> None:
    pytest.importorskip("cdshealpix")
    out = tmp_path / "plain.hats"
    write_frame(pl.DataFrame({"ra": [10.0], "dec": [0.0]}), out)
    assert "_union_ra" not in _read_hats(out, tmp_path).columns


def _sky_pair(tmp_path: Path) -> tuple[Path, Path]:
    left = tmp_path / "left.parquet"
    right = tmp_path / "right.parquet"
    # The nearer counterpart (0.2") has a very different magnitude; the
    # farther one (0.6") matches in magnitude.
    pl.DataFrame({"id": [1], "ra": [10.0], "dec": [0.0], "mag": [15.0]}).write_parquet(left)
    pl.DataFrame(
        {
            "rid": [100, 200],
            "ra": [10.0, 10.0],
            "dec": [0.2 / 3600.0, -0.6 / 3600.0],
            "mag": [19.0, 15.05],
        }
    ).write_parquet(right)
    return left, right


@pytest.mark.parametrize("matcher_kwargs", [{"extra_distance_cols": {"mag": 1.0}}, {}])
def test_auto_does_not_pick_stilts_for_a_request_it_cannot_honour(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, matcher_kwargs: dict
) -> None:
    left, right = _sky_pair(tmp_path)
    monkeypatch.setattr(stilts_mod, "stilts_available", lambda *a, **k: True)
    called: list[bool] = []

    def fake_stilts(*args, **kwargs):  # pragma: no cover - must not run here
        called.append(True)
        raise AssertionError("STILTS must not run for an unsupported request")

    monkeypatch.setattr(stilts_mod, "stilts_sky_match", fake_stilts)
    kwargs = {"radius_arcsec": 1.0, "fallback_policy": "error", **matcher_kwargs}
    if not matcher_kwargs:
        kwargs["filter_expr"] = "mag_2 > 0"
    result = CrossMatch().crossmatch(str(left), str(right), engine="auto", **kwargs)
    assert not called
    assert result.height == 1


def test_explicit_stilts_still_refuses_an_unsupported_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from xmatcher.exceptions import CrossMatchError

    left, right = _sky_pair(tmp_path)
    monkeypatch.setattr(stilts_mod, "stilts_available", lambda *a, **k: True)
    with pytest.raises(CrossMatchError, match="cannot honor"):
        CrossMatch().crossmatch(
            str(left),
            str(right),
            engine="stilts",
            radius_arcsec=1.0,
            fallback_policy="error",
            extra_distance_cols={"mag": 1.0},
        )


@pytest.mark.parametrize("engine", ["astropy", "fast"])
def test_extra_distance_cols_pick_the_photometric_counterpart(tmp_path: Path, engine: str) -> None:
    left, right = _sky_pair(tmp_path)
    plain = CrossMatch().crossmatch(str(left), str(right), engine=engine, radius_arcsec=1.0)
    assert plain["rid"].to_list() == [100]
    ranked = CrossMatch().crossmatch(
        str(left),
        str(right),
        engine=engine,
        radius_arcsec=1.0,
        extra_distance_cols={"mag": 1.0},
    )
    assert ranked["rid"].to_list() == [200]


def _hats_input(tmp_path: Path, name: str, frame: pl.DataFrame, ra: str, dec: str) -> Path:
    src = tmp_path / f"{name}.parquet"
    frame.write_parquet(src)
    tree = tmp_path / f"{name}.hats"
    convert(src, tree, ra_column=ra, dec_column=dec)
    return tree


def test_union_with_hats_inputs_uses_their_declared_position_columns(tmp_path: Path) -> None:
    pytest.importorskip("cdshealpix")
    a = _hats_input(
        tmp_path,
        "a_in",
        pl.DataFrame({"id": [1, 2], "RA_ICRS": [10.0, 20.0], "DE_ICRS": [0.0, 1.0]}),
        "RA_ICRS",
        "DE_ICRS",
    )
    b = tmp_path / "b_in.parquet"
    pl.DataFrame({"bid": [7, 8], "ra": [10.0001, 50.0], "dec": [0.0, 5.0]}).write_parquet(b)
    c = _hats_input(
        tmp_path,
        "c_in",
        pl.DataFrame({"cid": [9, 10], "RAJ2000": [20.0001, 80.0], "DEJ2000": [1.0, -5.0]}),
        "RAJ2000",
        "DEJ2000",
    )
    pair = tmp_path / "pair.hats"
    CrossMatch().crossmatch(str(a), str(b), radius_arcsec=1.0, join_type="1or2", output_file=pair)
    back = _read_hats(pair, tmp_path).sort("_union_ra")
    assert back.select("_union_ra", "_union_dec").rows() == [(10.0, 0.0), (20.0, 1.0), (50.0, 5.0)]

    multi = tmp_path / "multi.hats"
    CrossMatch().union_match([str(a), str(b), str(c)], radius_arcsec=1.0, output_file=multi)
    back = _read_hats(multi, tmp_path).sort("_union_ra")
    assert back["_union_ra"].to_list() == [10.0, 20.0, 50.0, 80.0]
    assert back["_src_cats"].to_list() == ["1+2", "1+3", "2", "3"]


def test_right_only_join_with_differently_named_columns_writes_hats(tmp_path: Path) -> None:
    pytest.importorskip("cdshealpix")
    a = tmp_path / "a2.parquet"
    b = tmp_path / "b2.parquet"
    pl.DataFrame({"id": [1], "ra": [10.0], "dec": [0.0]}).write_parquet(a)
    pl.DataFrame({"bid": [7, 8], "RA_ICRS": [10.0001, 50.0], "DE_ICRS": [0.0, 5.0]}).write_parquet(
        b
    )
    out = tmp_path / "only2.hats"
    CrossMatch().crossmatch(str(a), str(b), radius_arcsec=1.0, join_type="2not1", output_file=out)
    back = _read_hats(out, tmp_path)
    assert back["bid"].to_list() == [8]
    assert back.select("_union_ra", "_union_dec").rows() == [(50.0, 5.0)]


def test_existing_union_columns_are_kept(tmp_path: Path) -> None:
    pytest.importorskip("cdshealpix")
    frame = pl.DataFrame(
        {
            "ra": [10.0, None],
            "dec": [0.0, None],
            "_union_ra": [111.0, None],
            "_union_dec": [11.0, None],
            "ra_2": [10.0001, 30.0],
            "dec_2": [0.0, 3.0],
        }
    )
    out = tmp_path / "kept.hats"
    write_frame(frame, out, fallback_positions=[("ra_2", "dec_2")])
    back = _read_hats(out, tmp_path).sort("_union_ra")
    assert back.select("_union_ra", "_union_dec").rows() == [(30.0, 3.0), (111.0, 11.0)]


def test_trailing_slash_hats_output_gets_union_positions(
    three_catalogues: list[Path], tmp_path: Path
) -> None:
    pytest.importorskip("cdshealpix")
    a, b, _ = three_catalogues
    out = tmp_path / "slash.hats"
    CrossMatch().crossmatch(
        str(a), str(b), radius_arcsec=1.0, join_type="1or2", output_file=f"{out}/"
    )
    assert _read_hats(out, tmp_path).height == 3


def test_inner_join_hats_output_keeps_its_schema(
    three_catalogues: list[Path], tmp_path: Path
) -> None:
    pytest.importorskip("cdshealpix")
    a, b, _ = three_catalogues
    out = tmp_path / "inner.hats"
    CrossMatch().crossmatch(str(a), str(b), radius_arcsec=1.0, output_file=out)
    back = _read_hats(out, tmp_path)
    assert back.height == 1
    assert "_union_ra" not in back.columns

"""Release-audit regressions for the collaborator-facing CLI and engine wrappers."""

import polars as pl
import pytest

from xmatcher.cli import main


@pytest.fixture
def pair(tmp_path, monkeypatch):
    """Two small catalogues whose coordinate columns differ only in case."""
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.chdir(tmp_path)
    a = tmp_path / "a.csv"
    b = tmp_path / "b.csv"
    pl.DataFrame(
        {"id": [1, 2, 3], "ra": [10.0, 10.001, 11.0], "dec": [20.0, 20.001, 21.0]}
    ).write_csv(a)
    pl.DataFrame(
        {"sid": [7, 8, 9], "RA": [10.0001, 11.0002, 50.0], "DEC": [20.0001, 21.0, 50.0]}
    ).write_csv(b)
    return a, b


def test_cli_union_keeps_unmatched_rows(pair, tmp_path):
    a, b = pair
    out = tmp_path / "union.parquet"
    rc = main([str(a), str(b), "--union", "-r", "2", "--engine", "fast", "-o", str(out)])
    assert rc == 0
    result = pl.read_parquet(out)
    assert result.height == 4
    assert sorted(result["_src_cats"].to_list()) == ["1", "1+2", "1+2", "2"]


def test_cli_explicit_join_still_wins_for_pairwise_match(pair, tmp_path):
    a, b = pair
    out = tmp_path / "outer.parquet"
    rc = main([str(a), str(b), "--join", "1or2", "-r", "2", "--engine", "fast", "-o", str(out)])
    assert rc == 0
    assert pl.read_parquet(out).height == 4

"""Release-audit regressions for the collaborator-facing CLI and engine wrappers."""

from pathlib import Path

import polars as pl
import pytest

from xmatcher.cli import main
from xmatcher.request import MatchRequest


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


@pytest.mark.parametrize(
    "flags",
    [
        ["-r", "0"],
        ["-r", "-1"],
        ["-r", "nan"],
        ["-r", "inf"],
        ["--max-error", "0"],
        ["--lr-q", "2"],
        ["--batch-size", "0"],
        ["--ra", "10", "--dec", "95", "--radius-deg", "1"],
        ["--ra", "10", "--dec", "5", "--radius-deg", "0"],
        ["--ra", "10"],
        ["--radius-deg", "0.5"],
    ],
)
def test_cli_rejects_invalid_numeric_flags_without_traceback(pair, capsys, flags):
    a, b = pair
    with pytest.raises(SystemExit) as exc:
        main([str(a), str(b), *flags])
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "Traceback" not in err
    assert "error:" in err


def test_match_request_rejects_partial_region():
    with pytest.raises(ValueError, match="together"):
        MatchRequest.from_params("a.csv", "b.csv", ra=10.0, dec=5.0)


def test_readme_quickstart_runs_on_local_files(tmp_path, monkeypatch):
    """The README CLI and Python quickstarts run as written on two local files."""
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.chdir(tmp_path)
    pl.DataFrame({"ra": [10.0, 20.0], "dec": [5.0, 6.0], "g": [18.0, 19.0]}).write_parquet(
        "optical.parquet"
    )
    pl.DataFrame({"ra": [10.0001, 20.0001], "dec": [5.0, 6.0], "w1": [15.0, 16.0]}).write_csv(
        "infrared.csv"
    )
    assert (
        main(["match", "optical.parquet", "infrared.csv", "-r", "1.5", "-o", "matched.parquet"])
        == 0
    )
    assert pl.read_parquet(Path("matched.parquet")).height == 2

    from xmatcher import CrossMatch

    df = CrossMatch().crossmatch("optical.parquet", "infrared.csv", radius_arcsec=1.5)
    assert len(df) == 2

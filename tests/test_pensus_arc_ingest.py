"""Offline tests for the pensus ARC ingest scripts (Gaia XP continuous, CatWISE2020)."""

from __future__ import annotations

import gzip
import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

ARC = Path(__file__).resolve().parents[1] / "scripts" / "pensus" / "arc"
sys.path.insert(0, str(ARC))

import catwise_convert  # noqa: E402
import xp_convert  # noqa: E402

XP_COLUMNS = [
    "source_id",
    "solution_id",
    *(
        f"{b}_{c}"
        for b in ("bp", "rp")
        for c in (
            "basis_function_id",
            "degrees_of_freedom",
            "n_parameters",
            "n_measurements",
            "n_rejected_measurements",
            "standard_deviation",
            "chi_squared",
            "coefficients",
            "coefficient_errors",
            "coefficient_correlations",
            "n_relevant_bases",
            "relative_shrinking",
        )
    ),
]


def _xp_csv(path: Path, rows: list[dict]) -> None:
    def cell(v):
        if isinstance(v, list):
            return '"[' + ",".join(repr(x) for x in v) + ']"'
        return "" if v is None else str(v)

    lines = ["# %ECSV 1.0", "# ---"] + [",".join(XP_COLUMNS)]
    lines += [",".join(cell(r.get(c)) for c in XP_COLUMNS) for r in rows]
    with gzip.open(path, "wt") as fh:
        fh.write("\n".join(lines) + "\n")


def test_xp_convert_keeps_archive_dtypes_exactly(tmp_path):
    rng = np.random.default_rng(0)
    rows = []
    for i in range(3):
        coeff = rng.normal(size=55).tolist()
        errs = [float(np.float32(x)) for x in rng.random(55)]
        corr = [float(np.float32(x)) for x in rng.uniform(-1, 1, 1485)]
        r = {"source_id": 4295806720 + i, "solution_id": 4545469030156206081}
        for b in ("bp", "rp"):
            r |= {
                f"{b}_basis_function_id": 56,
                f"{b}_degrees_of_freedom": 931,
                f"{b}_n_parameters": 55,
                f"{b}_n_measurements": 986,
                f"{b}_n_rejected_measurements": 10,
                f"{b}_standard_deviation": 0.9934199,
                f"{b}_chi_squared": 918.78815,
                f"{b}_coefficients": coeff,
                f"{b}_coefficient_errors": errs,
                f"{b}_coefficient_correlations": corr,
                f"{b}_n_relevant_bases": 43,
                f"{b}_relative_shrinking": 0.25,
            }
        if i == 2:
            for c in ("coefficients", "coefficient_errors", "coefficient_correlations"):
                r[f"rp_{c}"] = None
        rows.append(r)
    src = tmp_path / "XpContinuousMeanSpectrum_000000-000001.csv.gz"
    _xp_csv(src, rows)
    table = xp_convert.convert(src)
    out = tmp_path / "x.parquet"
    xp_convert.write(table, out)
    back = pq.read_table(out)
    assert back.equals(table)
    assert back.schema.field("bp_coefficients").type == pa.list_(pa.float64())
    assert back.schema.field("bp_coefficient_correlations").type == pa.list_(pa.float32())
    assert back["bp_coefficients"][0].as_py() == rows[0]["bp_coefficients"]
    assert back["rp_coefficient_correlations"][1].as_py() == rows[1]["rp_coefficient_correlations"]
    assert back["rp_coefficients"][2].as_py() is None
    assert back["source_id"].to_pylist() == [r["source_id"] for r in rows]


def _ipac(
    path: Path,
    cols: list[tuple[str, str, int, str]],
    rows: list[list[str]],
    nsrc: int | None = None,
) -> None:
    def bar(vals):
        return "|" + "|".join(v.rjust(c[2]) for v, c in zip(vals, cols, strict=True)) + "|"

    lines = [
        f"\\Nsrc ={len(rows) if nsrc is None else nsrc}",
        bar([c[0] for c in cols]),
        bar([c[1] for c in cols]),
        bar(["--" for _ in cols]),
        bar([c[3] for c in cols]),
    ]
    lines += [
        " " + " ".join(v.rjust(c[2]) for v, c in zip(r, cols, strict=True)) + " " for r in rows
    ]
    with gzip.open(path, "wt", encoding="latin1") as fh:
        fh.write("\n".join(lines) + "\n")


CW_COLS = [
    ("source_id", "char", 22, "--"),
    ("ra", "double", 12, "--"),
    ("dec", "double", 12, "--"),
    ("w1mpro", "r", 8, "null"),
    ("w1sky", "r", 10, "null"),
    ("nb", "i", 4, "--"),
    ("ka", "i", 2, "n"),
]


def test_catwise_convert_types_nulls_and_counts(tmp_path):
    src = tmp_path / "t_cat_b0.tbl.gz"
    _ipac(
        src,
        CW_COLS,
        [
            ["0990m167_b0-021311", "98.5882317", "-17.4157444", "15.499", "2.571", "2", "3"],
            ["0990m167_b0-111190", "98.8375806", "-17.4157122", "null", "-1603.125", "1", "n"],
        ],
    )
    table, nsrc = catwise_convert.read_ipac(src)
    assert nsrc == table.num_rows == 2
    assert table["ra"].to_pylist() == [98.5882317, 98.8375806]
    assert table.schema.field("w1mpro").type == pa.float32()
    assert table["w1mpro"].to_pylist()[1] is None
    assert table.schema.field("w1sky").type == pa.float64()
    assert table["w1sky"].to_pylist() == [2.571, -1603.125]
    assert table["nb"].to_pylist() == [2, 1]
    assert table["ka"].to_pylist() == [3, None]
    assert catwise_convert.main(str(src), str(tmp_path / "t.parquet")) == 0


def test_catwise_convert_fails_closed(tmp_path):
    lossy = tmp_path / "lossy_cat_b0.tbl.gz"
    _ipac(lossy, CW_COLS, [["a", "1.0", "2.0", "15.49912", "1.0", "1", "0"]])
    with pytest.raises(ValueError, match="w1mpro has 7 significant digits"):
        catwise_convert.read_ipac(lossy)
    short = tmp_path / "short_cat_b0.tbl.gz"
    _ipac(short, CW_COLS, [["a", "1.0", "2.0", "15.5", "1.0", "1", "0"]], nsrc=2)
    assert catwise_convert.main(str(short), str(tmp_path / "s.parquet")) == 1
    assert not (tmp_path / "s.parquet").exists()


def _gaia_and_xp(tmp_path: Path, n: int, bogus: bool):
    u = pytest.importorskip("astropy.units")
    coords = pytest.importorskip("astropy.coordinates")
    healpix = pytest.importorskip("cdshealpix.nested")
    rng = np.random.default_rng(3)
    ra = rng.uniform(0, 360, n)
    dec = np.degrees(np.arcsin(rng.uniform(-1, 1, n)))
    hp29 = healpix.lonlat_to_healpix(
        coords.Longitude(ra * u.deg), coords.Latitude(dec * u.deg), 29
    ).astype(np.int64)
    hp12 = hp29 >> 34
    moved = rng.random(n) < 0.01
    nb = healpix.neighbours(hp12[moved].astype(np.uint64), 12)
    hp12_id = hp12.copy()
    hp12_id[moved] = np.array([r[r >= 0][1] for r in nb], dtype=np.int64)
    source_id = (hp12_id << 35) + np.arange(n)
    gaia = tmp_path / "gaia"
    parts = [(2, p) for p in range(192) if p != 17] + [(3, c) for c in range(68, 72)]
    for o, p in parts:
        m = (hp29 >> (2 * (29 - o))) == p
        d = gaia / "dataset" / f"Norder={o}" / "Dir=0"
        d.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table(
                {"source_id": source_id[m], "ra": ra[m], "dec": dec[m], "_healpix_29": hp29[m]}
            ),
            d / f"Npix={p}.parquet",
        )
    (gaia / "partition_info.csv").write_text(
        "Norder,Npix\n" + "".join(f"{o},{p}\n" for o, p in parts)
    )
    (gaia / "properties").write_text("hats_creation_date=2026-10-26T23:54UTC\n")
    keep = rng.random(n) < 0.7
    sid = np.sort(source_id[keep])
    lvl8 = sid >> 43
    edges = np.linspace(0, 12 * 4**8, 5).astype(np.int64)
    stage = tmp_path / "staging" / "stage1"
    (stage / "_done").mkdir(parents=True)
    names = []
    for i in range(4):
        ids = sid[(lvl8 >= edges[i]) & (lvl8 < edges[i + 1])]
        if bogus and i == 3:
            ids = np.append(ids, np.int64(((12 * 4**12 - 1) << 35) + 10**9))
        name = f"XpContinuousMeanSpectrum_{edges[i]:06d}-{edges[i + 1] - 1:06d}"
        coeffs = pa.array(
            [[float(s % 1000) + 0.123456789012] * 3 for s in ids], pa.list_(pa.float64())
        )
        pq.write_table(
            pa.table({"source_id": ids, "bp_coefficients": coeffs}), stage / f"{name}.parquet"
        )
        (stage / "_done" / f"{name}.csv.gz.json").write_text(json.dumps({"rows": len(ids)}))
        names.append(name)
    (stage.parent / "_MD5SUM.txt").write_text("".join(f"0  {nm}.csv.gz\n" for nm in names))
    return gaia, stage, names, int(keep.sum()), set(source_id[moved & keep].tolist())


def _layout(*args) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(ARC / "xp_layout.py"), *map(str, args)], capture_output=True, text=True
    )


def test_xp_layout_on_gaia_partitions(tmp_path):
    gaia, stage, names, expected, moved = _gaia_and_xp(tmp_path, 40_000, bogus=False)
    out = tmp_path / "out"
    assert _layout("run", gaia, stage, out, 0, 1, 2).returncode == 0
    fin = _layout("finalize", gaia, stage, out, expected)
    assert fin.returncode == 0, fin.stdout + fin.stderr
    tables = []
    for f in sorted((out / "dataset").glob("Norder=*/Dir=*/Npix=*/*.parquet")):
        order, pix = int(f.parents[2].name.split("=")[1]), int(f.parent.name.split("=")[1])
        t = pq.read_table(f)
        assert ((t["_healpix_29"].to_numpy() >> (2 * (29 - order))) == pix).all()
        tables.append(t)
    allt = pa.concat_tables(tables)
    assert allt.num_rows == expected == pc.count_distinct(allt["source_id"]).as_py()
    assert moved <= set(allt["source_id"].to_pylist())
    first = np.array([v[0] for v in allt["bp_coefficients"].to_pylist()])
    assert np.array_equal(first, (allt["source_id"].to_numpy() % 1000) + 0.123456789012)
    assert not list(stage.glob("*.parquet"))
    props = (out / "hats.properties").read_text()
    assert f"hats_nrows={expected}" in props and "hats_npix_suffix=/" in props


def test_xp_layout_fails_closed_on_unmatched_source(tmp_path):
    gaia, stage, names, expected, _ = _gaia_and_xp(tmp_path, 20_000, bogus=True)
    out = tmp_path / "out"
    run = _layout("run", gaia, stage, out, 0, 1, 2)
    assert run.returncode == 1 and f"FAIL {names[-1]}" in run.stdout
    assert (stage / f"{names[-1]}.parquet").exists()
    assert _layout("finalize", gaia, stage, out, expected).returncode == 1
    assert not (out / "partition_info.csv").exists()


def test_extract_cone_equals_bruteforce_across_partition_layouts(tmp_path):
    u = pytest.importorskip("astropy.units")
    coords = pytest.importorskip("astropy.coordinates")
    healpix = pytest.importorskip("cdshealpix.nested")
    pytest.importorskip("polars")
    sys.path.insert(0, str(ARC.parent))
    import canfar_pinned_match as pm

    rng = np.random.default_rng(5)
    n = 200_000
    ra = rng.uniform(0, 360, n)
    dec = np.degrees(np.arcsin(rng.uniform(-1, 1, n)))
    hp29 = healpix.lonlat_to_healpix(
        coords.Longitude(ra * u.deg), coords.Latitude(dec * u.deg), 29
    ).astype(np.int64)
    root = tmp_path / "cat"
    pix2 = hp29 >> 54
    for p in np.unique(pix2):
        m = pix2 == p
        t = pa.table({"ra": ra[m], "dec": dec[m], "_healpix_29": hp29[m], "x": rng.random(m.sum())})
        d = root / "dataset" / "Norder=2" / "Dir=0"
        if p % 2:
            (d / f"Npix={p}").mkdir(parents=True, exist_ok=True)
            half = t.num_rows // 2
            pq.write_table(t.slice(0, half), d / f"Npix={p}" / "a.parquet")
            pq.write_table(t.slice(half), d / f"Npix={p}" / "b.parquet")
        else:
            d.mkdir(parents=True, exist_ok=True)
            pq.write_table(t, d / f"Npix={p}.parquet")
    (root / "hats.properties").write_text("hats_col_ra=ra\nhats_col_dec=dec\n")
    for cra, cdec, rad in ((10.0, -30.0, 4.0), (200.0, 60.0, 7.5), (61.2, -48.4, 2.0)):
        info = pm.extract_cone(root, tmp_path / "cone.parquet", cra, cdec, rad)
        got = pq.read_table(tmp_path / "cone.parquet")
        r0, d0, r1, d1 = map(np.radians, (cra, cdec, ra, dec))
        sep = np.degrees(
            np.arccos(
                np.clip(np.sin(d0) * np.sin(d1) + np.cos(d0) * np.cos(d1) * np.cos(r1 - r0), -1, 1)
            )
        )
        want = set(hp29[sep <= rad].tolist())
        assert info["rows"] == got.num_rows == len(want) > 0
        assert set(got["_healpix_29"].to_pylist()) == want


def _catwise_rows(rng, tile: str, n: int) -> pa.Table:
    return pa.table(
        {
            "source_id": [f"{tile}_b0-{i:06d}" for i in rng.permutation(n) + 1],
            "ra": np.round(rng.uniform(0, 360, n), 7),
            "dec": np.round(rng.uniform(-90, 90, n), 7).astype(np.float64),
            "w1mpro": rng.random(n).astype(np.float32),
        }
    )


def _run_verify(root: Path, rows: int, glob: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(ARC / "verify_catwise_hats.py"), str(root), str(rows), glob],
        capture_output=True,
        text=True,
    )


def test_verify_catwise_hats_exact_conservation(tmp_path):
    pytest.importorskip("polars")
    rng = np.random.default_rng(11)
    stage = tmp_path / "stage1" / "000"
    stage.mkdir(parents=True)
    tables = [
        _catwise_rows(rng, t, n)
        for t, n in (("0000m061", 500), ("3599p863", 300), ("0015p000", 200))
    ]
    for i, t in enumerate(tables):
        pq.write_table(t, stage / f"t{i}.parquet")
    allrows = pa.concat_tables(tables)
    shuffled = allrows.take(rng.permutation(allrows.num_rows))
    glob = str(tmp_path / "stage1" / "*" / "*.parquet")

    def hats(table: pa.Table, name: str) -> Path:
        root = tmp_path / name
        for j, part in enumerate((table.slice(0, 400), table.slice(400))):
            d = root / "dataset" / "Norder=1" / "Dir=0"
            d.mkdir(parents=True, exist_ok=True)
            pq.write_table(part, d / f"Npix={j}.parquet")
        return root

    good = _run_verify(hats(shuffled, "good"), 1000, glob)
    assert good.returncode == 0, good.stdout + good.stderr
    assert json.loads(good.stdout)["checks"]["output_ids_distinct"]

    dup = pa.concat_tables([shuffled.slice(0, 999), shuffled.slice(0, 1)])
    bad = _run_verify(hats(dup, "dup"), 1000, glob)
    assert bad.returncode == 1
    checks = json.loads(bad.stdout)["checks"]
    assert not checks["output_ids_distinct"] and checks["rows"]

    assert _run_verify(hats(shuffled.slice(1), "short"), 1000, glob).returncode == 1

    moved = shuffled.set_column(
        1, "ra", pa.array(np.round(shuffled["ra"].to_numpy() + 1e-7 * (np.arange(1000) == 3), 7))
    )
    assert not json.loads(_run_verify(hats(moved, "moved"), 1000, glob).stdout)["checks"]["ra_e7"]

    pq.write_table(_catwise_rows(rng, "00x0m061", 3), stage / "bad.parquet")
    malformed = _run_verify(hats(shuffled, "good2"), 1000, glob)
    assert malformed.returncode != 0 and "source_id outside" in malformed.stderr


def test_ls_convert_mirror_schema_and_fail_closed(tmp_path):
    torch = pytest.importorskip("torch")
    torchfits = pytest.importorskip("torchfits")
    ls_convert = pytest.importorskip("ls_convert")

    rng = np.random.default_rng(3)
    n = 1000
    south = {
        "OBJID": torch.arange(n, dtype=torch.int32),
        "RA": torch.from_numpy(rng.uniform(0, 360, n)),
        "DEC": torch.from_numpy(rng.uniform(-90, 90, n)),
        "FLUX_G": torch.from_numpy(rng.random(n).astype(np.float32)),
        "FLUX_I": torch.from_numpy(rng.random(n).astype(np.float32)),
    }
    north = {k: v for k, v in south.items() if k != "FLUX_I"}
    torchfits.table.write(str(tmp_path / "s.fits"), south)
    torchfits.table.write(str(tmp_path / "n.fits"), north)
    s_schema = next(iter(torchfits.table.scan(str(tmp_path / "s.fits"), hdu=1))).schema
    schema = ls_convert.mirror_schema(s_schema.remove_metadata())

    assert ls_convert.convert(tmp_path / "s.fits", tmp_path / "s.parquet", "S", schema) == n
    assert ls_convert.convert(tmp_path / "n.fits", tmp_path / "n.parquet", "N", schema) == n
    s, nt = pq.read_table(tmp_path / "s.parquet"), pq.read_table(tmp_path / "n.parquet")
    assert s.schema.equals(schema) and nt.schema.equals(schema)
    for c in south:
        np.testing.assert_array_equal(s[c].to_numpy(), south[c].numpy())
    for c in north:
        np.testing.assert_array_equal(nt[c].to_numpy(), north[c].numpy())
    assert nt["FLUX_I"].null_count == n and s["FLUX_I"].null_count == 0
    assert set(s["PHOTSYS"].to_pylist()) == {"S"} and set(nt["PHOTSYS"].to_pylist()) == {"N"}

    with pytest.raises(ValueError, match="missing column FLUX_I"):
        ls_convert.convert(tmp_path / "n.fits", tmp_path / "x.parquet", "S", schema)
    no_g = {k: v for k, v in north.items() if k != "FLUX_G"}
    torchfits.table.write(str(tmp_path / "bad.fits"), no_g)
    with pytest.raises(ValueError, match="missing column FLUX_G"):
        ls_convert.convert(tmp_path / "bad.fits", tmp_path / "x.parquet", "N", schema)
    wrong = dict(south, OBJID=south["OBJID"].to(torch.int64))
    torchfits.table.write(str(tmp_path / "wrong.fits"), wrong)
    with pytest.raises(ValueError, match="OBJID"):
        ls_convert.convert(tmp_path / "wrong.fits", tmp_path / "x.parquet", "S", schema)

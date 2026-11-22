#!/usr/bin/env python3
"""Download SDSS DR16 PHOTO_SWEEP stargal-primary calibObj files (PhotoPrimary-style).

Preferred source (preferences): https://data.sdss.org/sas/dr16/eboss/sweeps/dr13_final/
Default runtime mirror when data.sdss.org TLS is down:
  https://dr16.sdss.org/sas/dr16/eboss/sweeps/dr13_final/

Skips sky / star / gal / wise-stargal-primary. Verifies against sweeps_dr13_final.sha1sum.
Pin target after build/promote: /arc/projects/hats/sdss/dr16 (not a sweeps path).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import re
import time
import urllib.error
import urllib.request

DEFAULT_BASE = os.environ.get(
    "PENSUS_SDSS_SAS_BASE",
    "https://dr16.sdss.org/sas/dr16/eboss/sweeps/dr13_final",
)
NAME_RE = re.compile(r'href="(calibObj-\d+-\d+-stargal-primary\.fits\.gz)"')


def _urls(base: str) -> tuple[str, str, str]:
    root = base.rstrip("/")
    return root, f"{root}/301/", f"{root}/sweeps_dr13_final.sha1sum"


def fetch(url: str, dest: pathlib.Path, expected: int | None = None) -> int:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".partial")
    existing = tmp.stat().st_size if tmp.exists() else 0
    headers = {}
    mode = "wb"
    if existing > 0:
        headers["Range"] = f"bytes={existing}-"
        mode = "ab"
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=120) as resp, open(tmp, mode) as fh:
        while True:
            chunk = resp.read(8 * 1024 * 1024)
            if not chunk:
                break
            fh.write(chunk)
    size = tmp.stat().st_size
    if expected is not None and size != expected:
        raise RuntimeError(f"size mismatch {dest.name}: got {size} expected {expected}")
    tmp.replace(dest)
    return size


def sha1_file(path: pathlib.Path) -> str:
    h = hashlib.sha1()
    with path.open("rb") as fh:
        while True:
            b = fh.read(8 * 1024 * 1024)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def load_sha1(sha1_url: str) -> dict[str, str]:
    text = urllib.request.urlopen(sha1_url, timeout=120).read().decode()
    out: dict[str, str] = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        digest, rel = parts[0], parts[1].lstrip("./")
        name = pathlib.Path(rel).name
        if name.endswith("-stargal-primary.fits.gz") and "wise-" not in name:
            out[name] = digest
    return out


def list_files(index_url: str) -> list[tuple[str, int]]:
    html = urllib.request.urlopen(index_url, timeout=180).read().decode("latin1", "replace")
    pat = re.compile(
        r'<a href="(calibObj-\d+-\d+-stargal-primary\.fits\.gz)">[^<]*</a>\s+\S+\s+\S+\s+(\d+)'
    )
    rows = [(n, int(s)) for n, s in pat.findall(html) if "wise-" not in n]
    if not rows:
        names = sorted(set(NAME_RE.findall(html)))
        rows = [(n, -1) for n in names if "wise-" not in n]
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="raw directory for fits.gz")
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--base", default=DEFAULT_BASE)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    _root, index_url, sha1_url = _urls(args.base)
    out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    print(f"source_base={args.base}", flush=True)
    print("loading sha1 map", flush=True)
    sha = load_sha1(sha1_url)
    print(f"sha1 entries={len(sha)}", flush=True)
    files = list_files(index_url)
    if args.limit > 0:
        files = files[: args.limit]
    print(f"files_to_fetch={len(files)}", flush=True)
    man_path = pathlib.Path(args.manifest)
    man_path.parent.mkdir(parents=True, exist_ok=True)
    done = json.loads(man_path.read_text()) if man_path.exists() else {"files": {}}
    ok = fail = skip = 0
    bytes_ok = 0
    for i, (name, expected) in enumerate(files, 1):
        dest = out / name
        if dest.exists() and dest.stat().st_size > 0:
            digest = sha1_file(dest)
            want = sha.get(name)
            if want and digest != want:
                print(f"FAIL sha1 existing {name}", flush=True)
                dest.unlink()
            else:
                skip += 1
                bytes_ok += dest.stat().st_size
                done["files"][name] = {
                    "bytes": dest.stat().st_size,
                    "sha1": digest,
                    "ok": True,
                }
                continue
        url = index_url + name
        for attempt in range(5):
            try:
                size = fetch(url, dest, expected if expected > 0 else None)
                digest = sha1_file(dest)
                want = sha.get(name)
                if want and digest != want:
                    raise RuntimeError(f"sha1 mismatch {name}")
                done["files"][name] = {"bytes": size, "sha1": digest, "ok": True}
                ok += 1
                bytes_ok += size
                break
            except (urllib.error.URLError, TimeoutError, RuntimeError, OSError) as exc:
                print(f"retry {attempt + 1} {name}: {exc}", flush=True)
                time.sleep(2**attempt)
        else:
            fail += 1
            done["files"][name] = {"ok": False}
        if i % 25 == 0 or i == len(files):
            man_path.write_text(json.dumps(done, indent=2, sort_keys=True) + "\n")
            print(
                f"progress {i}/{len(files)} ok={ok} skip={skip} fail={fail} "
                f"gib={bytes_ok / 2**30:.1f}",
                flush=True,
            )
    done["summary"] = {
        "ok": ok,
        "skip": skip,
        "fail": fail,
        "bytes": bytes_ok,
        "n_expected": len(files),
        "source": index_url,
        "product": "calibObj-*-stargal-primary (PhotoPrimary-style)",
        "planned_hats_path": "/arc/projects/hats/sdss/dr16",
    }
    man_path.write_text(json.dumps(done, indent=2, sort_keys=True) + "\n")
    print(json.dumps(done["summary"], indent=2))
    return 0 if fail == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())

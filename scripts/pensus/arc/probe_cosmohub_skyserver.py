#!/usr/bin/env python3
"""Live-probe CosmoHub + SDSS SkyServer/CasJobs with bashrc creds (no secret printing).

Writes a JSON summary under stage TMP and prints Day-0-relevant findings for XMA-20.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import pathlib
import sys
import urllib.error
import urllib.request


def flag(name: str) -> None:
    val = os.environ.get(name, "")
    print(f"{name}_set={'yes' if val else 'no'} {name}_len={len(val)}", flush=True)


def http_json(url: str, method: str = "GET", data: dict | None = None, headers: dict | None = None, timeout: int = 60):
    body = None if data is None else json.dumps(data).encode()
    req = urllib.request.Request(
        url,
        data=body,
        method=method,
        headers={"User-Agent": "pensus-probe/1.0", "Content-Type": "application/json", **(headers or {})},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        return resp.status, json.loads(raw.decode() if raw else "{}")


def probe_skyserver() -> dict:
    out: dict = {"service": "skyserver_casjobs", "ok": False}
    user = os.environ.get("SKYSERVER_USERID", "")
    pw = os.environ.get("SKYSERVER_PW", "")
    if not user or not pw:
        out["error"] = "missing SKYSERVER_USERID/PW"
        return out
    # SciServer login → token (username/password; not numeric WSID)
    try:
        status, payload = http_json(
            "https://apps.sciserver.org/login-portal/keystone/v2/tokens",
            method="POST",
            data={"auth": {"identity": {"password": {"user": {"name": user, "password": pw}}}}},
            timeout=45,
        )
        token = (
            payload.get("access", {})
            .get("token", {})
            .get("id")
            or payload.get("token", {}).get("id")
        )
        out["login_http"] = status
        out["token_present"] = bool(token)
        if not token:
            # Alternate SciServer login form used by SciScript
            status2, payload2 = http_json(
                "https://apps.sciserver.org/login-portal/keystone/v3/auth/tokens",
                method="POST",
                data={
                    "auth": {
                        "identity": {
                            "methods": ["password"],
                            "password": {
                                "user": {
                                    "name": user,
                                    "domain": {"id": "default"},
                                    "password": pw,
                                }
                            },
                        }
                    }
                },
                timeout=45,
            )
            out["login_v3_http"] = status2
            # token often in header; urllib may not expose easily — mark attempt
            out["login_v3_body_keys"] = sorted(payload2.keys()) if isinstance(payload2, dict) else []
        # Quick anonymous SkyServer sqlSearch (public) as reachability
        qurl = (
            "https://skyserver.sdss.org/dr16/SkyServerWS/SearchTools/SqlSearch"
            "?format=json&cmd=" + urllib.request.quote("SELECT TOP 3 objID, ra, dec, mode FROM PhotoPrimary")
        )
        req = urllib.request.Request(qurl, headers={"User-Agent": "pensus-probe/1.0"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            sample = resp.read(800).decode("utf-8", "replace")
        out["photo_primary_sample_ok"] = "objID" in sample or "objid" in sample.lower() or "[" in sample
        out["photo_primary_head"] = sample[:200]
        out["day0_note"] = (
            "CDS HATS preferred when up; SkyServer/CasJobs usable for PhotoPrimary "
            "RA-strip dumps to pin hats/sdss/dr16 while CDS/SAS down."
        )
        out["ok"] = True
    except Exception as exc:  # noqa: BLE001
        out["error"] = f"{type(exc).__name__}: {exc}"
    return out


def probe_cosmohub() -> dict:
    out: dict = {"service": "cosmohub", "ok": False}
    user = os.environ.get("COSMOHUB_USERID", "")
    pw = os.environ.get("COSMOHUB_PW", "")
    if not user or not pw:
        out["error"] = "missing COSMOHUB_USERID/PW"
        return out
    # CosmoHub API login (PIC)
    for login_url in (
        "https://cosmohub.pic.es/api/v1/auth/login/",
        "https://cosmohub.pic.es/api/auth/login/",
        "https://cosmohub.pic.es/api/v1/users/login/",
    ):
        try:
            status, payload = http_json(
                login_url,
                method="POST",
                data={"username": user, "email": user, "password": pw},
                timeout=45,
            )
            out["login_url"] = login_url
            out["login_http"] = status
            out["login_keys"] = sorted(payload.keys()) if isinstance(payload, dict) else []
            token = (
                payload.get("token")
                or payload.get("key")
                or payload.get("access")
                or (payload.get("data") or {}).get("token")
            )
            out["token_present"] = bool(token)
            # Catalog list endpoints vary by CosmoHub version
            if token:
                for cat_url in (
                    "https://cosmohub.pic.es/api/v1/catalogs/",
                    "https://cosmohub.pic.es/api/catalogs/",
                    "https://cosmohub.pic.es/api/v1/datasets/",
                ):
                    try:
                        req = urllib.request.Request(
                            cat_url,
                            headers={
                                "User-Agent": "pensus-probe/1.0",
                                "Authorization": f"Token {token}",
                                "Accept": "application/json",
                            },
                        )
                        with urllib.request.urlopen(req, timeout=60) as resp:
                            cats = json.loads(resp.read().decode())
                        names: list[str] = []
                        items = cats if isinstance(cats, list) else cats.get("results") or cats.get("data") or []
                        for it in items[:80]:
                            if isinstance(it, dict):
                                names.append(str(it.get("name") or it.get("title") or it.get("slug") or ""))
                            else:
                                names.append(str(it))
                        out["catalog_endpoint"] = cat_url
                        out["catalog_count_sample"] = len(names)
                        out["catalog_names_sample"] = [n for n in names if n][:40]
                        # Day-0 photometry-ish keywords
                        keys = ("pauc", "kids", "jpas", "j-pas", "euclid", "des", "sdss", "gaia", "wise", "vhs")
                        hits = [n for n in names if any(k in n.lower() for k in keys)]
                        out["day0_keyword_hits"] = hits[:30]
                        out["ok"] = True
                        break
                    except Exception as exc:  # noqa: BLE001
                        out.setdefault("catalog_errors", []).append(f"{cat_url}: {type(exc).__name__}: {exc}")
            if out.get("ok"):
                break
        except Exception as exc:  # noqa: BLE001
            out.setdefault("login_errors", []).append(f"{login_url}: {type(exc).__name__}: {exc}")
    if not out.get("ok") and "error" not in out:
        out["error"] = "login or catalog list failed; see login_errors/catalog_errors"
    out["day0_note"] = (
        "CosmoHub available for authenticated catalog pulls; do not launch unless a Day-0 "
        "survey lacks a better public mirror this turn."
    )
    return out


def main() -> int:
    for n in (
        "SKYSERVER_USERID",
        "SKYSERVER_PW",
        "COSMOHUB_USERID",
        "COSMOHUB_PW",
        "HSC_USERID",
        "HSC_PW",
    ):
        flag(n)
    report = {
        "probed_at_utc": dt.datetime.now(dt.UTC).isoformat(),
        "skyserver": probe_skyserver(),
        "cosmohub": probe_cosmohub(),
    }
    out = pathlib.Path(
        os.environ.get(
            "PENSUS_PROBE_OUT",
            "/arc/projects/k-pop/pensus/logs/cosmohub_skyserver_probe.json",
        )
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True), flush=True)
    print(f"wrote {out}", flush=True)
    return 0 if report["skyserver"].get("ok") or report["cosmohub"].get("ok") else 1


if __name__ == "__main__":
    # Avoid unused import lint for urllib.error in some environments
    _ = urllib.error
    sys.exit(main())

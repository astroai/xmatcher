"""Resolve ``hats:<short_name>`` against the IVOA HATS registry.

HATS is a storage format, not a naming authority, so a HATS tree is normally
named by its location.  The IVOA registry does publish HATS services with a
``short_name`` and an access URL, which is the closest thing to a catalogue
namespace; this module resolves those names and caches the answer, so a name
that resolved once keeps working when the registry is unreachable.  HTTPS,
VOSpace, ``s3://`` and Hugging Face (``hf://datasets/…``) trees are resolved;
anything else is reported as unreadable rather than substituted with a
different copy.

Resolution is deliberately strict: an exact ``short_name`` (or resource title)
match wins, a unique prefix is accepted, and anything ambiguous is an error that
lists the candidates.  A wrong catalogue here would silently match the wrong
sky, which is worse than making the user pass a URL.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

from .exceptions import InputError
from .storage import default_cache_root

logger = logging.getLogger(__name__)

#: Cache lifetime for the registry snapshot (the registry changes slowly).
DEFAULT_TTL_SECONDS = 7 * 24 * 3600
_CACHE_VERSION = 1
_READABLE_SCHEMES = ("http://", "https://", "vos:", "s3://", "hf://")


def cache_file(cache_root: str | Path | None = None) -> Path:
    """Return the JSON snapshot path for the HATS registry."""
    root = Path(cache_root or default_cache_root()).expanduser()
    return root / "registry" / "hats_registry.json"


def _read_cache(path: Path) -> dict[str, Any] | None:
    try:
        snapshot = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(snapshot, dict) or snapshot.get("version") != _CACHE_VERSION:
        return None
    if not isinstance(snapshot.get("records"), list):
        return None
    return snapshot


def _write_cache(path: Path, records: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": _CACHE_VERSION,
        "fetched_at": time.time(),
        "records": records,
    }
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        tmp.write_text(json.dumps(payload, indent=1, sort_keys=True))
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def _registry_records() -> list[dict[str, str]]:
    """Query the IVOA registry for HATS services.

    Kept as a separate function so tests can replace the network call without
    installing a fake VO registry.
    """
    try:
        from pyvo import registry  # noqa: PLC0415 - optional at import time
    except ImportError as exc:  # pragma: no cover - pyvo is a hard dependency
        raise InputError(
            "HATS name lookup needs pyvo; install it with `pip install pyvo`."
        ) from exc

    try:
        result = registry.search(servicetype="hats")
    except Exception as exc:  # noqa: BLE001 - pyvo raises a wide variety
        raise InputError(
            f"The IVOA HATS registry is unreachable ({exc}). Pass an explicit "
            "`hats:<https://…>` URL, or retry when the network is available."
        ) from exc

    records: list[dict[str, str]] = []
    for record in result:
        try:
            url = (record.access_url or "").strip()
        except Exception:  # noqa: BLE001 - a broken record must not fail the query
            url = ""
        if not url:
            continue
        records.append(
            {
                "short_name": str(getattr(record, "short_name", "") or "").strip(),
                "title": str(getattr(record, "res_title", "") or "").strip(),
                "url": url,
            }
        )
    return records


def hats_catalogues(
    *,
    cache_root: str | Path | None = None,
    ttl: float = DEFAULT_TTL_SECONDS,
    refresh: bool = False,
) -> dict[str, list[dict[str, str]]]:
    """Return ``{name: [record, …]}`` for the registered HATS catalogues.

    ``name`` is the lower-cased registry short name; records keep the registry
    spelling, the resource title, and the access URL.  A cached snapshot is
    used when it is younger than ``ttl`` or when the registry cannot be
    reached, so offline runs keep resolving names that resolved before.
    """
    path = cache_file(cache_root)
    snapshot = None if refresh else _read_cache(path)
    if snapshot is not None and (time.time() - float(snapshot.get("fetched_at", 0))) < ttl:
        records = snapshot["records"]
    else:
        try:
            records = _registry_records()
        except InputError:
            if snapshot is None:
                raise
            records = snapshot["records"]
        else:
            try:
                _write_cache(path, records)
            except OSError as exc:
                # A read-only cache root (shared /arc mounts, containers) must
                # not make name resolution fail: the lookup still worked.
                logger.debug("cannot cache the HATS registry at %s: %s", path, exc)

    catalogues: dict[str, list[dict[str, str]]] = {}
    for record in records:
        key = str(record.get("short_name", "")).strip().casefold()
        if key:
            catalogues.setdefault(key, []).append(record)
    return catalogues


def _preferred(records: list[dict[str, str]]) -> tuple[str, str] | None:
    """Pick the entry a mirror can actually read; ``None`` when none is."""
    for record in records:
        url = record["url"]
        if url.startswith(_READABLE_SCHEMES):
            return record.get("short_name", ""), url
    return None


def resolve_hats_short_name(
    name: str,
    *,
    cache_root: str | Path | None = None,
    ttl: float = DEFAULT_TTL_SECONDS,
    refresh: bool = False,
) -> str:
    """Resolve a registry HATS name to a URL the mirror can read."""
    query = name.strip().casefold()
    if not query:
        raise InputError("hats: identifiers need a registry name or a URL.")

    catalogues = hats_catalogues(cache_root=cache_root, ttl=ttl, refresh=refresh)
    matches = list(catalogues.get(query, []))

    if not matches:
        by_title = [
            records
            for records in catalogues.values()
            if any(str(record.get("title", "")).strip().casefold() == query for record in records)
        ]
        if len(by_title) == 1:
            matches = by_title[0]
        elif len(by_title) > 1:
            raise InputError(
                f"'{name}' is the title of {len(by_title)} registered HATS catalogues; "
                "pass the short name instead."
            )

    if not matches:
        prefix = [records for key, records in catalogues.items() if key.startswith(query)]
        if len(prefix) == 1:
            matches = prefix[0]
        elif len(prefix) > 1:
            names = sorted(
                {record.get("short_name", "") for records in prefix for record in records}
            )
            raise InputError(
                f"'{name}' matches {len(names)} registered HATS catalogues: "
                f"{', '.join(names[:8])}{' …' if len(names) > 8 else ''}. "
                "Use the full short name."
            )

    if not matches:
        raise InputError(
            f"No HATS catalogue named '{name}' in the IVOA registry "
            f"({len(catalogues)} registered). Run `xmatcher list --hats` for the "
            "current names, or pass `hats:<https://…>`."
        )

    preferred = _preferred(matches)
    if preferred is None:
        url = matches[0]["url"]
        raise InputError(
            f"HATS catalogue '{matches[0].get('short_name', name)}' is registered as "
            f"{url!r}, which xmatcher cannot read: only HTTP(S), VOSpace, S3 (s3://) "
            "and Hugging Face (hf://datasets/…) trees are mirrored, and this entry is "
            "none of those. Look for an HTTP(S) copy (LSDB's data.lsdb.io mirrors most "
            "public catalogues) and pass it as `hats:<https://…>`."
        )
    return preferred[1]


__all__ = [
    "DEFAULT_TTL_SECONDS",
    "cache_file",
    "hats_catalogues",
    "resolve_hats_short_name",
]

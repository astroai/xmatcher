"""Read-only community HATS trees used in place of a mirror.

A *community root* (configured via ``cache.community_roots`` or
``XMATCHER_COMMUNITY_ROOTS``) holds HATS trees that other people maintain,
laid out by survey and product (``2mass/psc``, ``gaia/dr3``, ...) rather
than by xmatcher's cache layout. xmatcher never writes there.  Two
mechanisms find an equivalent copy of a configured catalogue:

* **Sync records.**  A tree copied with xmatcher-style versioning carries a
  ``sync.json`` recording the upstream ``source`` URL and the ``version``
  directory name (:func:`xmatcher.mirror._version_dir`).
  :func:`community_index` scans the community roots for these records and
  caches the index under the writable cache root
  (``<cache>/registry/community_index.json``) with a TTL; the modification
  times of each root and its first-level directories are a cheap invalidation
  signature, so a new survey or product directory is noticed before the TTL
  expires.  A remote HATS catalogue whose ``access_identifier`` and version
  match a record uses that tree.
* **Explicit local paths.**  A catalogue entry may list ``local_paths:`` for
  equivalent copies without a sync record.

Either way the copy must pass :func:`verify_tree` (``properties`` present, a
partition index present, a complete sync record when there is one, and the
entry's pinned ``hats_nrows`` / ``obs_collection`` / coordinate columns
consistent with the tree's properties).  The copy is then read in place and
the catalogue entry's metadata (epoch, motion, error, id and default columns)
is kept.  Otherwise the caller falls back to the writable cache/mirror.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from .sources import CatalogueSource, is_remote_hats_uri

logger = logging.getLogger(__name__)

#: Sync-record file names recognised in a community tree.
SYNC_RECORD_NAMES = ("sync.json",)
#: HATS properties file names, in lookup order.
PROPERTIES_NAMES = ("properties", "hats.properties")
_PARTITION_INDEX = ("partition_info.csv", "dataset/partition_info.parquet", "dataset/_metadata")
_INDEX_VERSION = 1
DEFAULT_INDEX_TTL_S = 24 * 3600.0
_MAX_DEPTH = 4
_SKIP_PREFIXES = (".", "Norder=", "Dir=", "dataset", "_")


@dataclass(frozen=True)
class ReadOnlyCopy:
    """A verified tree that serves a catalogue in place."""

    path: Path
    origin: str  # "community" (sync record) or "local_paths"
    root: str | None = None


# --------------------------------------------------------------------------- #
# properties + verification
# --------------------------------------------------------------------------- #
def read_properties(tree: Path) -> dict[str, str] | None:
    """Parse ``tree``'s HATS properties; ``None`` when there are none."""
    for name in PROPERTIES_NAMES:
        path = tree / name
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        out: dict[str, str] = {}
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            out[key.strip()] = value.strip()
        return out
    return None


def _record_complete(record: dict[str, Any], tree: Path) -> str | None:
    """Return a reason when a sync record does not describe a complete copy."""
    if not record.get("fetched_at"):
        return "its sync record has no fetched_at (last copy incomplete)"
    files = record.get("files")
    if isinstance(files, dict):
        for name in (*PROPERTIES_NAMES, "partition_info.csv"):
            entry = files.get(name)
            size = entry.get("size") if isinstance(entry, dict) else None
            if isinstance(size, int) and size >= 0:
                try:
                    actual = (tree / name).stat().st_size
                except OSError:
                    return f"{name} listed in its sync record is missing"
                if actual != size:
                    return f"{name} is {actual} bytes, its sync record says {size}"
    return None


def verify_tree(
    tree: str | Path,
    src: CatalogueSource,
    *,
    record: dict[str, Any] | None = None,
) -> tuple[bool, str]:
    """Check that ``tree`` is a complete HATS copy consistent with ``src``.

    Returns ``(ok, reason)``.  Only cheap metadata reads are made (no data
    partition is opened).
    """
    path = Path(tree)
    if not path.is_dir():
        return False, "not a directory"
    props = read_properties(path)
    if props is None:
        return False, "no HATS properties file"
    if not any((path / rel).exists() for rel in _PARTITION_INDEX):
        return False, "no partition index (partition_info.csv / dataset/_metadata)"
    if record is not None:
        reason = _record_complete(record, path)
        if reason:
            return False, reason
    if src.expected_hats_nrows is not None:
        nrows = props.get("hats_nrows")
        if nrows is None or str(nrows).strip() != str(int(src.expected_hats_nrows)):
            return False, f"hats_nrows={nrows}, the catalogue pins {src.expected_hats_nrows}"
    if src.expected_obs_collection is not None:
        obs = props.get("obs_collection")
        if obs != src.expected_obs_collection:
            return False, (
                f"obs_collection={obs!r}, the catalogue pins {src.expected_obs_collection!r}"
            )
    for key, column in (("hats_col_ra", src.ra_column), ("hats_col_dec", src.dec_column)):
        declared = props.get(key)
        if declared and column and declared != column:
            return False, f"{key}={declared!r} but the catalogue uses {column!r}"
    return True, "ok"


# --------------------------------------------------------------------------- #
# index of sync records
# --------------------------------------------------------------------------- #
def _read_record(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    source, version = data.get("source"), data.get("version")
    if not isinstance(source, str) or not isinstance(version, str):
        return None
    return data


def scan_root(root: str | Path, *, max_depth: int = _MAX_DEPTH) -> list[dict[str, Any]]:
    """Walk ``root`` for HATS trees carrying a sync record (directories only).

    The walk stops at a tree (a directory with HATS properties) and never
    enters ``dataset``/``Norder=`` subtrees, so it stats directories, not
    partitions.
    """
    base = Path(root)
    found: list[dict[str, Any]] = []

    def walk(directory: Path, depth: int) -> None:
        try:
            entries = list(os.scandir(directory))
        except OSError:
            return
        names = {e.name for e in entries}
        if any(name in names for name in PROPERTIES_NAMES):
            for record_name in SYNC_RECORD_NAMES:
                if record_name in names:
                    record = _read_record(directory / record_name)
                    if record is not None:
                        found.append(
                            {
                                "path": str(directory),
                                "record": record_name,
                                "source": record["source"],
                                "version": record["version"],
                                "fetched_at": record.get("fetched_at"),
                            }
                        )
                        break
            return
        if depth >= max_depth:
            return
        for entry in sorted(entries, key=lambda e: e.name):
            if entry.name.startswith(_SKIP_PREFIXES):
                continue
            try:
                if entry.is_dir():
                    walk(Path(entry.path), depth + 1)
            except OSError:
                continue

    walk(base, 0)
    return found


def _signature(root: str) -> str:
    """Cheap change signature: mtimes of ``root`` and its first-level dirs."""
    parts: list[str] = []
    try:
        parts.append(f".:{os.stat(root).st_mtime_ns}")
        for entry in sorted(os.scandir(root), key=lambda e: e.name):
            if entry.name.startswith(".") or not entry.is_dir():
                continue
            parts.append(f"{entry.name}:{entry.stat().st_mtime_ns}")
    except OSError:
        return "missing"
    return hashlib.sha1("\n".join(parts).encode()).hexdigest()


def index_file(cache_root: str | None) -> Path | None:
    """Path of the cached index under the writable cache root (POSIX only)."""
    if not cache_root or str(cache_root).startswith("vos:"):
        return None
    return (
        Path(os.path.expandvars(str(cache_root))).expanduser()
        / "registry"
        / ("community_index.json")
    )


def _index_ttl() -> float:
    env = os.environ.get("XMATCHER_COMMUNITY_INDEX_TTL")
    try:
        return float(env) if env else DEFAULT_INDEX_TTL_S
    except ValueError:
        return DEFAULT_INDEX_TTL_S


def community_index(
    roots: list[str],
    *,
    cache_root: str | None = None,
    ttl_s: float | None = None,
    refresh: bool = False,
) -> list[dict[str, Any]]:
    """Sync records found under the community ``roots`` (cached, see module doc)."""
    from .storage import assert_writable  # noqa: PLC0415 - avoid an import cycle

    ttl = _index_ttl() if ttl_s is None else float(ttl_s)
    path = index_file(cache_root)
    cached: dict[str, Any] = {}
    if path is not None and not refresh:
        try:
            data = json.loads(path.read_text())
            if isinstance(data, dict) and data.get("version") == _INDEX_VERSION:
                cached = data.get("roots") or {}
        except (OSError, ValueError):
            cached = {}
    now = time.time()
    records: list[dict[str, Any]] = []
    updated: dict[str, Any] = {}
    changed = False
    for root in roots:
        if root.startswith("vos:") or not os.path.isdir(root):
            continue
        sig = _signature(root)
        entry = cached.get(root)
        if (
            isinstance(entry, dict)
            and entry.get("signature") == sig
            and 0.0 <= now - float(entry.get("scanned_at", 0.0)) < ttl
            and isinstance(entry.get("records"), list)
        ):
            updated[root] = entry
        else:
            updated[root] = {"signature": sig, "scanned_at": now, "records": scan_root(root)}
            changed = True
        records.extend(updated[root]["records"])
    if path is not None and changed:
        try:
            assert_writable(path.parent, "community index directory")
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
            tmp.write_text(
                json.dumps({"version": _INDEX_VERSION, "roots": updated}, indent=1, sort_keys=True)
            )
            os.replace(tmp, path)
        except Exception as exc:  # noqa: BLE001 - the index is a cache only
            logger.warning("could not write the community index %s: %s", path, exc)
    return records


# --------------------------------------------------------------------------- #
# resolution
# --------------------------------------------------------------------------- #
def _norm_url(url: str) -> str:
    return url.rstrip("/")


def find_read_only_copy(
    src: CatalogueSource,
    *,
    read_roots: list[str] | None = None,
    cache_root: str | None = None,
) -> ReadOnlyCopy | None:
    """Return a verified read-only copy serving ``src`` in place, else ``None``.

    ``local_paths`` on the catalogue entry are tried first, then sync records
    under the community ``read_roots`` (default :func:`storage.community_roots`)
    whose ``source`` and ``version`` match the catalogue's ``access_identifier``
    and version directory.  Rejected candidates are logged with the reason.
    """
    from .mirror import _version_dir  # noqa: PLC0415 - avoid an import cycle
    from .storage import community_roots  # noqa: PLC0415

    for raw in src.local_paths or []:
        text = os.path.expandvars(os.path.expanduser(str(raw)))
        if "$" in text:
            continue
        ok, reason = verify_tree(text, src)
        if ok:
            return ReadOnlyCopy(Path(text), "local_paths")
        if Path(text).exists():
            logger.warning("%s: local path %s is not usable (%s)", src.name, text, reason)
        else:
            logger.debug("%s: local path %s does not exist", src.name, text)

    if src.access_method != "hats" or not is_remote_hats_uri(src.access_identifier):
        return None
    roots = community_roots() if read_roots is None else list(read_roots)
    if not roots:
        return None
    url = _norm_url(str(src.access_identifier))
    version = _version_dir(src)
    for record in community_index(roots, cache_root=cache_root):
        if _norm_url(str(record.get("source", ""))) != url:
            continue
        tree = Path(record["path"])
        if record.get("version") != version:
            logger.warning(
                "%s: community tree %s records version %s, expected %s; not used",
                src.name,
                tree,
                record.get("version"),
                version,
            )
            continue
        full = _read_record(tree / str(record.get("record", SYNC_RECORD_NAMES[0])))
        ok, reason = verify_tree(tree, src, record=full or {})
        if ok:
            root = next((r for r in roots if str(tree).startswith(r.rstrip("/") + "/")), None)
            return ReadOnlyCopy(tree, "community", root)
        logger.warning("%s: community tree %s is not usable (%s)", src.name, tree, reason)
    return None


def read_only_source(src: CatalogueSource, copy: ReadOnlyCopy) -> CatalogueSource:
    """``src`` repointed at a read-only local HATS tree (entry metadata kept)."""
    return replace(
        src,
        is_local=False,
        access_method="hats",
        access_identifier=str(copy.path),
        path=copy.path,
        hats_cache_rel=None,
        hats_cache_root=None,
        read_only_origin=copy.origin,
        _frame=None,
    )


def resolve_in_place(
    src: CatalogueSource,
    *,
    read_roots: list[str] | None = None,
    cache_root: str | None = None,
) -> CatalogueSource | None:
    """Return ``src`` served from a verified read-only copy, logging which one."""
    if src.read_only_origin:
        return src
    copy = find_read_only_copy(src, read_roots=read_roots, cache_root=cache_root)
    if copy is None:
        return None
    logger.info(
        "%s: reading %s copy %s in place (read-only; no mirror)",
        src.name,
        "community" if copy.origin == "community" else "local_paths",
        copy.path,
    )
    return read_only_source(src, copy)


# --------------------------------------------------------------------------- #
# drafting catalogue entries (``xmatcher adopt hats``)
# --------------------------------------------------------------------------- #
_ID_CANDIDATES = (
    "source_id",
    "objID",
    "object_id",
    "objid",
    "sourceid",
    "cntr",
    "id",
    "ID",
    "designation",
)
_PM_CANDIDATES = (("pmra", "pmdec"), ("PMRA", "PMDEC"), ("pmRA", "pmDE"), ("PMRA", "PMDec"))
_PLX_CANDIDATES = ("parallax", "PARALLAX", "Plx", "plx")
_ERR_CANDIDATES = (
    ("ra_error", "dec_error", "mas"),
    ("sigra", "sigdec", "arcsec"),
    ("raMeanErr", "decMeanErr", "arcsec"),
    ("e_raj2000", "e_dej2000", None),
    ("e_RA_ICRS", "e_DE_ICRS", None),
)
_EPOCH_COLUMN_CANDIDATES = ("ref_epoch", "REF_EPOCH")


def _parse_properties_text(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        out[key.strip()] = value.strip()
    return out


def _remote_properties(url: str) -> dict[str, str] | None:
    import urllib.request  # noqa: PLC0415

    for rel in PROPERTIES_NAMES:
        try:
            with urllib.request.urlopen(f"{url.rstrip('/')}/{rel}", timeout=30) as resp:
                body = resp.read(1 << 20).decode("utf-8", errors="replace")
        except Exception:  # noqa: BLE001 - try the next name
            continue
        props = _parse_properties_text(body)
        if "hats_nrows" in props or "hats_col_ra" in props:
            return props
    return None


def _local_schema(tree: Path) -> list[str] | None:
    path = tree / "dataset" / "_common_metadata"
    if not path.is_file():
        return None
    try:
        import pyarrow.parquet as pq  # noqa: PLC0415

        return list(pq.read_schema(path).names)
    except Exception:  # noqa: BLE001 - a draft without a schema is still useful
        return None


def draft_hats_entry(
    location: str, *, name: str | None = None
) -> tuple[str, dict[str, Any], list[str]]:
    """Draft a catalogue entry from a HATS tree's properties (and schema).

    ``location`` is a local tree path or a remote HATS URL.  Returns
    ``(name, entry, notes)``; ``notes`` lists the fields that could not be
    verified from metadata and need checking by hand (units, epochs).
    """
    from .mirror import _safe_name  # noqa: PLC0415

    notes: list[str] = []
    remote = is_remote_hats_uri(location)
    schema: list[str] | None = None
    record: dict[str, Any] | None = None
    if remote:
        if not location.startswith(("http://", "https://")):
            raise ValueError("adopt hats reads remote properties over http(s) only")
        props = _remote_properties(location)
        if props is None:
            raise ValueError(f"no HATS properties at {location}")
        notes.append("schema not read (remote tree): column names come from properties only")
    else:
        tree = Path(os.path.expandvars(os.path.expanduser(location))).resolve()
        props = read_properties(tree)
        if props is None:
            raise ValueError(f"no HATS properties under {tree}")
        schema = _local_schema(tree)
        if schema is None:
            notes.append("schema not read (no dataset/_common_metadata)")
        for record_name in SYNC_RECORD_NAMES:
            record = _read_record(tree / record_name)
            if record is not None:
                break
        location = str(tree)
    cols = set(schema or [])

    def pick(*candidates: str) -> str | None:
        return next((c for c in candidates if c in cols), None)

    obs = props.get("obs_collection")
    key = name or _safe_name(obs or Path(location.rstrip("/")).name).lower()
    entry: dict[str, Any] = {
        "description": f"{obs or key} HATS tree ({location})",
        "archive": "hats",
        "service_id": "hats_service",
    }
    if record is not None:
        entry["access_identifier"] = record["source"]
        entry["local_paths"] = [location]
    else:
        entry["access_identifier"] = location
    ra = props.get("hats_col_ra") or pick("ra", "RA", "raj2000", "RA_ICRS")
    dec = props.get("hats_col_dec") or pick("dec", "DEC", "dej2000", "DE_ICRS")
    if not ra or not dec:
        notes.append("ra_column/dec_column: not declared in properties; set them by hand")
    entry["ra_column"] = ra or "ra"
    entry["dec_column"] = dec or "dec"
    survey_id = props.get("hats_cols_survey_id")
    ident = survey_id.split()[0] if survey_id else pick(*_ID_CANDIDATES)
    if ident:
        entry["id_column"] = ident
    else:
        notes.append("id_column: no survey id declared or recognised")
    epoch = props.get("hats_coordinate_epoch") or props.get("hats_epoch")
    if epoch:
        try:
            entry["epoch"] = float(epoch.lstrip("JjBb"))
        except ValueError:
            notes.append(f"epoch: cannot parse {epoch!r}")
    else:
        notes.append("epoch: not declared in properties; set the catalogue epoch by hand")
    epoch_col = pick(*_EPOCH_COLUMN_CANDIDATES)
    if epoch_col:
        entry["epoch_column"] = epoch_col
        notes.append(f"epoch_column {epoch_col!r}: confirm it holds Julian years")
    for pm_ra, pm_dec in _PM_CANDIDATES:
        if pm_ra in cols and pm_dec in cols:
            entry["pm_ra_column"], entry["pm_dec_column"] = pm_ra, pm_dec
            notes.append("pm columns: confirm mas/yr")
            break
    plx = pick(*_PLX_CANDIDATES)
    if plx:
        entry["parallax_column"] = plx
    for ra_err, dec_err, unit in _ERR_CANDIDATES:
        if ra_err in cols and dec_err in cols:
            entry["ra_err_column"], entry["dec_err_column"] = ra_err, dec_err
            if unit:
                entry["pos_err_units"] = unit
            notes.append(f"pos_err_units for {ra_err}/{dec_err}: confirm the unit")
            break
    nrows = props.get("hats_nrows")
    if nrows and nrows.isdigit():
        entry["estimated_size"] = int(nrows)
        entry["hats_nrows"] = int(nrows)
    if obs:
        entry["obs_collection"] = obs
    return key, entry, notes


__all__ = [
    "ReadOnlyCopy",
    "community_index",
    "draft_hats_entry",
    "find_read_only_copy",
    "read_only_source",
    "resolve_in_place",
    "scan_root",
    "verify_tree",
]

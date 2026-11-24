"""Storage abstraction for xmatcher mirrors and Ray task I/O.

A :class:`Storage` hides *where* the durable root lives (a POSIX dir on the
driver/manager node, or a CANFAR VOSpace ``vos:`` URI) behind one API.  All
catalogue parquet reads/writes and per-worker staging funnel through it, so
a Ray task reading "the same catalogue file" transparently hits the durable
root or a worker's fast scratch copy.

``LocalStorage`` is a plain pathlib-backed dir whose writes are durable and
atomic (same-directory temp file, ``fsync``, ``os.replace``). ``VOSpaceStorage`` shells the ``vos`` CLI
(``vls``/``vcp``/``vput``/``vmkdir``), falls back to the python ``vos``
package when the binary is missing, and raises :class:`ConfigError` when
neither is available. The python client path has been exercised against live
ARC VOSpace; the CLI flags remain unverified against a live endpoint, so
their argv is kept in one place (:meth:`VOSpaceStorage._argv`).
"""

from __future__ import annotations

import builtins
import contextlib
import logging
import os
import re
import secrets
import shutil
import subprocess
from pathlib import Path

import polars as pl

from .exceptions import ConfigError, ReadOnlyRootError

logger = logging.getLogger(__name__)


class Storage:
    """Protocol for a durable catalogue store (POSIX dir or VOSpace root)."""

    root: str | Path

    def exists(self, rel: str) -> bool:  # pragma: no cover - protocol
        raise NotImplementedError

    def size(self, rel: str) -> int:  # pragma: no cover - protocol
        """Size in bytes of ``rel``; ``-1`` when unknown/unavailable."""
        raise NotImplementedError

    def list(self, rel: str) -> builtins.list[str]:  # pragma: no cover - protocol
        """Entry names directly under ``rel`` (files and dirs)."""
        raise NotImplementedError

    def read_parquet(self, rel: str) -> pl.DataFrame:  # pragma: no cover - protocol
        raise NotImplementedError

    def parquet_schema(self, rel: str) -> dict[str, object]:  # pragma: no cover - protocol
        """Column name -> polars dtype for the parquet file at ``rel``.

        Used by the union plan builder, which must know every catalogue's
        columns before fanning out (a remote root cannot be probed with a
        local ``Path``).
        """
        raise NotImplementedError

    def write_parquet(self, df: pl.DataFrame, rel: str) -> None:  # pragma: no cover
        raise NotImplementedError

    def rename(self, src: str, dst: str) -> None:  # pragma: no cover - protocol
        raise NotImplementedError

    def mkdir(self, rel: str) -> None:  # pragma: no cover - protocol
        raise NotImplementedError

    def stage_out(self, local: Path, rel: str) -> None:  # pragma: no cover
        """Copy a local file into the durable root at ``rel``."""

    def stage_in(self, rel: str, local: Path) -> None:  # pragma: no cover
        """Copy ``rel`` from the durable root to a local file."""

    def rm(self, rel: str) -> None:  # pragma: no cover - protocol
        """Delete the node at ``rel`` (missing nodes are a no-op)."""


def _safe_rel(rel: str) -> str:
    """Normalize a storage rel, rejecting path traversal out of the root.

    Remote listings (``partition_info.parquet`` file_loc values, HTML hrefs)
    are server-controlled; a malicious/misconfigured source must not be able
    to write outside the cache root.
    """
    rel = rel.lstrip("/")
    if any(part == ".." for part in rel.split("/")):
        raise ValueError(f"storage rel escapes the root: {rel!r}")
    return rel


def _fsync(path: Path) -> None:
    """Flush ``path``'s data to stable storage before it is renamed into place.

    ``os.replace`` alone is atomic only in the namespace: after a power loss or
    node crash the new name can point at a zero-length or partly written file.
    """
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _tmp_path(target: Path) -> Path:
    """Create a unique same-directory temporary file for atomic replacement."""
    for _ in range(100):
        path = target.parent / f".{target.name}.{secrets.token_hex(8)}.tmp"
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o666)
        except FileExistsError:
            continue
        os.close(fd)
        return path
    raise FileExistsError(f"could not allocate a temporary file beside {target}")


class LocalStorage(Storage):
    """POSIX-directory :class:`Storage` implementation."""

    root: Path

    def __init__(self, root: str | Path) -> None:
        self.root = Path(os.path.expandvars(str(root))).expanduser()
        #: The read-only root holding this storage, if any.  Reads work as
        #: usual; every write method refuses (see :func:`assert_writable`).
        self.read_only_root = read_only_root_for(self.root)
        if not self.root.exists():
            self._check_writable()
            self.root.mkdir(parents=True, exist_ok=True)

    def _check_writable(self) -> None:
        if getattr(self, "read_only_root", None) is not None:
            raise ReadOnlyRootError(
                f"refusing to write under {self.root}: it is inside the read-only "
                f"community root {self.read_only_root}"
            )

    def _path(self, rel: str) -> Path:
        rel = _safe_rel(rel)
        if not rel:
            return self.root
        path = self.root / rel
        try:
            path.resolve().relative_to(self.root.resolve())
        except (OSError, RuntimeError, ValueError) as exc:
            raise ValueError(f"storage rel escapes the root: {rel!r}") from exc
        return path

    def exists(self, rel: str) -> bool:
        return self._path(rel).exists()

    def size(self, rel: str) -> int:
        p = self._path(rel)
        try:
            return p.stat().st_size if p.is_file() else -1
        except OSError:
            return -1

    def list(self, rel: str) -> builtins.list[str]:
        p = self._path(rel)
        if not p.is_dir():
            return []
        return sorted(entry.name for entry in p.iterdir())

    def read_parquet(self, rel: str) -> pl.DataFrame:
        return pl.read_parquet(self._path(rel))

    def parquet_schema(self, rel: str) -> dict[str, object]:
        return dict(pl.read_parquet_schema(self._path(rel)))

    def write_parquet(self, df: pl.DataFrame, rel: str) -> None:
        self._check_writable()
        target = self._path(rel)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = _tmp_path(target)
        try:
            df.write_parquet(tmp)
            _fsync(tmp)
            os.replace(tmp, target)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise

    def rename(self, src: str, dst: str) -> None:
        self._check_writable()
        src_p = self._path(src)
        dst_p = self._path(dst)
        dst_p.parent.mkdir(parents=True, exist_ok=True)
        os.replace(src_p, dst_p)

    def mkdir(self, rel: str) -> None:
        self._check_writable()
        self._path(rel).mkdir(parents=True, exist_ok=True)

    def stage_out(self, local: Path, rel: str) -> None:
        self._check_writable()
        target = self._path(rel)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = _tmp_path(target)
        try:
            shutil.copy2(local, tmp)
            _fsync(tmp)
            os.replace(tmp, target)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise

    def stage_in(self, rel: str, local: Path) -> None:
        shutil.copy2(self._path(rel), local)

    def rm(self, rel: str) -> None:
        self._check_writable()
        path = self._path(rel)
        if path.is_symlink():
            path.unlink()
        elif path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        elif path.exists():
            path.unlink()


class VOSpaceStorage(Storage):
    """CANFAR VOSpace :class:`Storage` via the ``vos`` CLI (or python binding).

    ``root`` is a ``vos:`` URI prefix.  Commands are resolved once at init:
    a ``vos`` binary on PATH wins, the python ``vos`` package is the
    fallback, and :class:`ConfigError` is raised when neither exists.

    The python client path has been exercised against live ARC VOSpace. The
    CLI flags have not been verified against that endpoint; its argv is
    centralized in :meth:`_argv`.
    """

    def __init__(self, root: str) -> None:
        if not isinstance(root, str) or not root.startswith("vos:"):
            raise ConfigError(f"VOSpaceStorage root must be a 'vos:' URI, got {root!r}")
        self.root = root.rstrip("/")
        #: VOSpace URIs naming the community HATS tree are read-only.
        self.read_only_root = _vos_read_only_root(self.root)
        self._binary: str | None = shutil.which("vos")
        self._client = None  # python-vos client when no binary
        if self._binary is None:
            try:
                import vos  # type: ignore

                self._client = vos.Client()
            except Exception as exc:
                raise ConfigError(
                    "VOSpace cache root requires the 'vos' CLI on PATH or the python "
                    f"'vos' package (neither available: {exc}). Install one to sync "
                    "with vos: roots."
                ) from exc
        self._ensure_root()

    # ------------------------------------------------------------------ CLI
    def _argv(self, cmd: str, *args: str) -> builtins.list[str]:
        """Build the ``vos`` CLI argv for ``cmd`` (centralised for adaptation)."""

        def uri(rel: str) -> str:
            return f"{self.root}/{_safe_rel(rel)}".replace("//", "/")

        if cmd == "list":
            return ["vos", "vls", uri(args[0] if args else "")]
        if cmd == "list-top":
            return ["vos", "vls", uri(args[0] if args else "")]
        if cmd == "mkdir":
            return ["vos", "vmkdir", "-p", uri(args[0] if args else "")]
        if cmd == "put":
            return ["vos", "vcp", args[0], uri(args[1])]
        if cmd == "get":
            return ["vos", "vcp", uri(args[0]), args[1]]
        if cmd == "mv":
            return ["vos", "vmv", uri(args[0]), uri(args[1])]
        if cmd == "rm":
            return ["vos", "vrm", uri(args[0] if args else "")]
        raise ValueError(f"unknown vos command {cmd!r}")

    def _run(self, argv: builtins.list[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            argv,
            capture_output=True,
            text=True,
            check=False,
        )

    def _uri(self, rel: str) -> str:
        return f"{self.root}/{_safe_rel(rel)}"

    def _check_writable(self) -> None:
        if getattr(self, "read_only_root", None) is not None:
            raise ReadOnlyRootError(
                f"refusing to write under {self.root}: it is inside the read-only "
                f"community root {self.read_only_root}"
            )

    def _ensure_root(self) -> None:
        if self._binary is not None:
            proc = self._run(["vos", "vls", str(self.root)])
            if proc.returncode != 0:
                self._check_writable()
                mk = self._run(["vos", "vmkdir", "-p", str(self.root)])
                if mk.returncode != 0:
                    raise ConfigError(
                        f"Cannot access/create VOSpace root {self.root}: {mk.stderr.strip()}"
                    )
        else:  # python-vos fallback
            try:
                self._client.get_node(self.root)  # type: ignore[union-attr]
            except Exception:
                self._check_writable()
                try:
                    self._client.mkdir(self.root)  # type: ignore[union-attr]
                except Exception as exc:
                    raise ConfigError(f"Cannot access VOSpace root {self.root}: {exc}") from exc

    def exists(self, rel: str) -> bool:
        if self._binary is not None:
            return self._run(self._argv("list-top", rel)).returncode == 0
        try:
            self._client.get_node(self._uri(rel))  # type: ignore[union-attr]
            return True
        except Exception:
            return False

    def size(self, rel: str) -> int:
        if self._binary is not None:
            # vls -l long listing: "<perms> <owner> <group> <size> <date> <name>"
            # (layout varies across clients, but the size column always follows
            # at least three leading non-numeric fields).  Take the FIRST
            # integer token at index >= 3 — the date token ("2024-01-01") is
            # not all-digits so it never wins.  Unparsable -> -1, and the
            # mirror's manifest logic treats -1 as "unknown, always refetch",
            # so accuracy here is best-effort, never correctness.
            proc = self._run(["vos", "vls", "-l", self._uri(rel)])
            if proc.returncode != 0:
                return -1
            for line in proc.stdout.splitlines():
                parts = line.split()
                for tok in parts[3:]:
                    if tok.isdigit():
                        return int(tok)
            return -1
        try:
            node = self._client.get_node(self._uri(rel))  # type: ignore[union-attr]
            size = getattr(node, "size", None)
            if size is None:
                size = (node.props or {}).get("length")  # type: ignore[union-attr]
            return int(size) if size is not None else -1
        except Exception:
            return -1

    def list(self, rel: str) -> builtins.list[str]:
        """Entry names directly under ``rel`` (non-recursive; dirs w/o trailing slash)."""
        if self._binary is not None:
            proc = self._run(self._argv("list", rel))
            if proc.returncode != 0:
                return []
            names = []
            for line in proc.stdout.splitlines():
                name = line.strip().rstrip("/").rsplit("/", 1)[-1]
                if name and name not in names:
                    names.append(name)
            return sorted(names)
        try:
            nodes = self._client.listdir(self._uri(rel))  # type: ignore[union-attr]
            names = []
            for node in nodes:
                path = str(node if isinstance(node, str) else node.uri)
                name = path.rstrip("/").rsplit("/", 1)[-1]
                if name:
                    names.append(name)
            return sorted(names)
        except Exception:
            return []

    def read_parquet(self, rel: str) -> pl.DataFrame:
        import tempfile

        tmpdir = tempfile.mkdtemp(prefix="xmatcher-vos-")
        local = Path(tmpdir) / "part.parquet"
        try:
            self.stage_in(rel, local)
            if not local.exists():
                raise FileNotFoundError(rel)
            return pl.read_parquet(local)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def parquet_schema(self, rel: str) -> dict[str, object]:
        """Stage the partition in and read its footer (one transfer, not two)."""
        import tempfile

        tmpdir = tempfile.mkdtemp(prefix="xmatcher-vos-schema-")
        local = Path(tmpdir) / "part.parquet"
        try:
            self.stage_in(rel, local)
            if not local.exists():
                raise FileNotFoundError(rel)
            return dict(pl.read_parquet_schema(local))
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def write_parquet(self, df: pl.DataFrame, rel: str) -> None:
        self._check_writable()
        import tempfile

        tmpdir = tempfile.mkdtemp(prefix="xmatcher-vos-")
        local = Path(tmpdir) / "part.parquet"
        try:
            df.write_parquet(local)
            self.stage_out(local, rel)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def rename(self, src: str, dst: str) -> None:
        self._check_writable()
        if self._binary is not None:
            proc = self._run(self._argv("mv", src, dst))
            if proc.returncode != 0:
                raise OSError(f"vos vmv failed: {proc.stderr.strip()}")
            return
        try:
            self._client.move(self._uri(src), self._uri(dst))  # type: ignore[union-attr]
        except Exception as exc:
            raise OSError(f"vos move failed: {exc}") from exc

    def mkdir(self, rel: str) -> None:
        self._check_writable()
        if self._binary is not None:
            proc = self._run(self._argv("mkdir", rel))
            if proc.returncode != 0:
                raise OSError(f"vos vmkdir failed: {proc.stderr.strip()}")
            return
        uri = self._uri(rel)
        try:
            node = self._client.get_node(uri)  # type: ignore[union-attr]
        except Exception:
            node = None
        if node is not None:
            if getattr(node, "type", None) == "vos:ContainerNode":
                return
            raise OSError(f"vos mkdir failed: {uri} exists and is not a container")
        try:
            self._client.mkdir(uri)  # type: ignore[union-attr]
        except Exception as exc:
            # A concurrent creator may win between get_node and mkdir. Match
            # the CLI's `vmkdir -p` behavior only if the resulting node is a
            # container; an existing data node is still an error.
            try:
                node = self._client.get_node(uri)  # type: ignore[union-attr]
            except Exception:
                raise OSError(f"vos mkdir failed: {exc}") from exc
            if getattr(node, "type", None) == "vos:ContainerNode":
                return
            raise OSError(f"vos mkdir failed: {exc}") from exc

    def _python_put(self, local: Path, rel: str) -> None:
        """Store a local file under ``rel`` via the python-vos fallback."""
        # The copy method was verified in a live python-vos 3.7 ARC VOSpace
        # smoke; CI covers the protocol through a fake client.
        try:
            self._client.copy(str(local), self._uri(rel))  # type: ignore[union-attr]
        except Exception as exc:
            raise OSError(f"vos vcp failed: {exc}") from exc

    def _in_get(self, rel: str, local: Path) -> None:
        try:
            self._client.copy(self._uri(rel), str(local))  # type: ignore[union-attr]
        except Exception as exc:
            raise OSError(f"vos vcp failed: {exc}") from exc

    def _mkdir_parents(self, rel: str) -> None:
        """Ensure the parent containers of ``rel`` exist (vcp does not)."""
        parent = rel.rsplit("/", 1)[0] if "/" in rel else ""
        self.mkdir(parent)

    def stage_out(self, local: Path, rel: str) -> None:
        self._check_writable()
        self._mkdir_parents(rel)
        if self._binary is not None:
            proc = self._run(self._argv("put", str(local), rel))
            if proc.returncode != 0:
                raise OSError(f"vos vcp failed: {proc.stderr.strip()}")
            return
        self._python_put(local, rel)

    def stage_in(self, rel: str, local: Path) -> None:
        if self._binary is not None:
            proc = self._run(self._argv("get", rel, str(local)))
            if proc.returncode != 0:
                raise OSError(f"vos vcp failed: {proc.stderr.strip()}")
            return
        self._in_get(rel, local)

    def rm(self, rel: str) -> None:
        self._check_writable()
        if self._binary is not None:
            proc = self._run(self._argv("rm", rel))
            if proc.returncode != 0 and "not found" not in proc.stderr.lower():
                # missing nodes are a no-op
                raise OSError(f"vos vrm failed: {proc.stderr.strip()}")
            return
        with contextlib.suppress(Exception):
            self._client.delete(self._uri(rel))  # type: ignore[union-attr]


def open_storage(root: str | Path) -> Storage:
    """Open a :class:`Storage` for ``root`` (``vos:`` URI → VOSpace, else local dir)."""
    if isinstance(root, str) and root.startswith("vos:"):
        return VOSpaceStorage(root)
    return LocalStorage(root)


def platform_arc_root() -> str | None:
    """``/arc`` when running on an AstroAI/CANFAR session, else ``None``.

    Every platform pod mounts the shared ``/arc`` volume; laptops never
    have it.  This is the on-platform probe for the storage defaults.
    """
    return "/arc" if os.path.isdir("/arc") else None


#: Platform cache root for mirrored catalogues. ``/arc/projects/hats`` is the
#: community tree (read it, never write to it); xmatcher's own mirrors live under
#: ``/arc/projects/astroai/hats`` until they are promoted to community-ready HATS.
PLATFORM_CACHE_ROOT = "/arc/projects/astroai/hats"
#: Platform root for crossmatch outputs (bare relative output names).
PLATFORM_OUTPUT_ROOT = "/arc/projects/astroai/xmatcher"
#: Community HATS tree on AstroAI/CANFAR sessions.  xmatcher reads trees
#: under it in place and never writes there; it is always a protected root,
#: whether or not it is configured as a community (read) root.
PLATFORM_COMMUNITY_ROOT = "/arc/projects/hats"

_REGISTERED_READ_ONLY: set[str] = set()
_VOS_COMMUNITY = re.compile(r"^vos:(?://[^/]*~arc/|arc:)?/?projects/hats(?:/|$)")


def _split_roots(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        items: list[object] = list(value.split(os.pathsep))
    elif isinstance(value, (list, tuple)):
        items = list(value)
    else:
        raise ConfigError(f"expected a list of paths, got {value!r}")
    out: list[str] = []
    for item in items:
        text = str(item).strip()
        if text and text not in out:
            out.append(text)
    return out


def community_roots(cache_cfg: dict | None = None) -> list[str]:
    """Read-only community roots whose HATS trees are used in place.

    ``XMATCHER_COMMUNITY_ROOTS`` (``os.pathsep``-separated; an empty value
    disables them) wins over ``cache_cfg["community_roots"]``; on
    AstroAI/CANFAR sessions the default is ``[/arc/projects/hats]`` when that
    directory exists, else ``[]``.  These trees are never synced, replicated
    or written to; an index of their sync records is kept under the writable
    cache root.
    """
    env = os.environ.get("XMATCHER_COMMUNITY_ROOTS")
    if env is not None:
        return _split_roots(env)
    cache_cfg = cache_cfg or {}
    if "community_roots" in cache_cfg:
        return _split_roots(cache_cfg.get("community_roots"))
    if platform_arc_root() and os.path.isdir(PLATFORM_COMMUNITY_ROOT):
        return [PLATFORM_COMMUNITY_ROOT]
    return []


def register_read_only_roots(roots: object) -> None:
    """Add ``roots`` to the process-wide protected (never written) set."""
    for root in _split_roots(roots):
        _REGISTERED_READ_ONLY.add(root)


def read_only_roots(cache_cfg: dict | None = None) -> list[str]:
    """Every protected root: community roots, registered roots, and
    :data:`PLATFORM_COMMUNITY_ROOT` (always)."""
    out = [PLATFORM_COMMUNITY_ROOT]
    for root in (*community_roots(cache_cfg), *sorted(_REGISTERED_READ_ONLY)):
        if root not in out:
            out.append(root)
    return out


def _vos_read_only_root(uri: str) -> str | None:
    return PLATFORM_COMMUNITY_ROOT if _VOS_COMMUNITY.match(uri) else None


def _inside(path: str, root: str) -> bool:
    for a, b in (
        (os.path.abspath(path), os.path.abspath(root)),
        (os.path.realpath(path), os.path.realpath(root)),
    ):
        if a == b or a.startswith(b.rstrip(os.sep) + os.sep):
            return True
    return False


def read_only_root_for(path: str | Path, cache_cfg: dict | None = None) -> str | None:
    """The protected root containing ``path`` (POSIX or ``vos:``), else ``None``."""
    text = os.path.expandvars(os.path.expanduser(str(path)))
    if text.startswith("vos:"):
        return _vos_read_only_root(text)
    for root in read_only_roots(cache_cfg):
        if root.startswith("vos:"):
            continue
        if _inside(text, os.path.expandvars(os.path.expanduser(root))):
            return root
    return None


def assert_writable(path: str | Path, label: str, cache_cfg: dict | None = None) -> None:
    """Raise :class:`ReadOnlyRootError` when ``path`` lies inside a read-only root."""
    root = read_only_root_for(path, cache_cfg)
    if root is not None:
        raise ReadOnlyRootError(
            f"{label} ({path}) is inside the read-only community root {root}; xmatcher "
            "never writes there. Use a writable cache root (XMATCHER_CACHE_ROOT, "
            f"default {PLATFORM_CACHE_ROOT} on CANFAR) or output directory instead."
        )


def default_cache_root() -> str:
    """Effective default cache root.

    ``XMATCHER_CACHE_ROOT`` wins; on AstroAI/CANFAR sessions (shared ``/arc``
    volume) the default is :data:`PLATFORM_CACHE_ROOT`
    (``/arc/projects/astroai/hats``) — never a home directory, which is
    pod-local and lost, and never the community tree ``/arc/projects/hats``,
    which is read-only for xmatcher; off the platform it falls back to
    ``~/.cache/xmatcher``.
    """
    env = os.environ.get("XMATCHER_CACHE_ROOT")
    if env:
        return env
    if platform_arc_root():
        return PLATFORM_CACHE_ROOT
    return str(Path.home() / ".cache" / "xmatcher")


def default_output_root() -> str | None:
    """Root for crossmatch outputs.

    ``XMATCHER_OUTPUT_ROOT`` wins (empty string disables); on
    AstroAI/CANFAR sessions the default is :data:`PLATFORM_OUTPUT_ROOT`
    (``/arc/projects/astroai/xmatcher``; never the community ``/arc/projects/hats``);
    off the platform there is no default and relative outputs stay
    cwd-relative.  The CLI resolves bare relative output names under this
    root; explicit paths are always respected verbatim.
    """
    env = os.environ.get("XMATCHER_OUTPUT_ROOT")
    if env is not None:
        return env.strip() or None
    if platform_arc_root():
        return PLATFORM_OUTPUT_ROOT
    return None


def replica_roots(cache_cfg: dict | None = None) -> list[str]:
    """Writable replica roots: ``cache_cfg["replica_roots"]`` then the legacy
    ``cache_cfg["roots"]`` (same meaning, kept for backward compatibility).

    Replicas receive best-effort copies of each completed sync and are probed
    after the primary by the ``--no-sync`` readers.  They are never read-only:
    a replica inside a community root is a configuration error.
    """
    cache_cfg = cache_cfg or {}
    out: list[str] = []
    for key in ("replica_roots", "roots"):
        for text in _split_roots(cache_cfg.get(key) or []):
            if text not in out:
                out.append(text)
    return out


def all_cache_roots(cache_cfg: dict | None = None) -> list[str]:
    """Ordered, deduplicated writable cache roots: primary first, then replicas.

    Primary = ``$XMATCHER_CACHE_ROOT`` → ``cache_cfg["root"]`` →
    :func:`default_cache_root`.  :func:`replica_roots` are appended after it
    (blank entries dropped; the primary is never duplicated as a replica).
    Never returns ``[]``.  Replicas may be ``vos:`` URIs — :func:`open_storage`
    routes those — or shared POSIX dirs; they are the mirror-fallback copies
    the union reads when the primary copy is gone.  Read-only community roots
    (:func:`community_roots`) are never part of this list; a primary or
    replica inside one raises :class:`ReadOnlyRootError`.
    """
    cache_cfg = cache_cfg or {}
    primary = os.environ.get("XMATCHER_CACHE_ROOT") or cache_cfg.get("root") or default_cache_root()
    roots = [str(primary)]
    for text in replica_roots(cache_cfg):
        if text not in roots:
            roots.append(text)
    for i, root in enumerate(roots):
        assert_writable(root, "cache root" if i == 0 else "replica cache root", cache_cfg)
    return roots


def assert_headroom(path: str | Path, min_free_gb: float, label: str) -> None:
    """Fail fast when ``path``'s filesystem has less than ``min_free_gb`` GiB free.

    Runs before any day-scale mirror/union starts so a multi-day run cannot
    die at 95% into a full disk. ``label`` names the mount in the error.
    The directory is created if missing (every caller writes into it next).
    A ``vos:`` root is not a local filesystem: its quota is not visible to
    ``statvfs``, so it is not checked (and no local ``vos:`` directory is
    created).
    """
    from .exceptions import CrossMatchError

    assert_writable(path, label)
    if str(path).startswith("vos:"):
        logger.debug("%s is a VOSpace root; its free space cannot be checked", label)
        return
    os.makedirs(str(path), exist_ok=True)
    free_gib = shutil.disk_usage(str(path)).free / 1024**3
    if free_gib < min_free_gb:
        raise CrossMatchError(
            f"{label} has only {free_gib:.1f} GiB free, below the --min-free-gb floor "
            f"of {min_free_gb:g} GiB; refusing to start a sync/union it cannot fit."
        )

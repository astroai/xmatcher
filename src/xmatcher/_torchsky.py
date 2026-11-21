"""Torchsky integration: one import point and one version floor.

Torchsky is a core dependency: it provides the exact sky candidate search of
the local engines, the ``engine="torchsky"`` matcher, the likelihood-ratio and
N-way association statistics (``associate``, ``associate_many``), the
epoch-propagation Jacobians and the HEALPix coverings.

An installed Torchsky older than :data:`MIN_VERSION` (or one that cannot be
imported) is an error rather than silently ignored: its APIs differ, and a
partial import would otherwise fail deep inside a match.
"""

from __future__ import annotations

import functools
import importlib
import importlib.util
import re
from types import ModuleType

from .exceptions import CrossMatchError

MIN_VERSION = (0, 6, 0)
_MIN_TEXT = ".".join(str(part) for part in MIN_VERSION)
INSTALL_HINT = "pip install --upgrade 'torchsky>=0.6.0'"


def installed() -> bool:
    """Whether a ``torchsky`` distribution is importable at all."""
    return importlib.util.find_spec("torchsky") is not None


@functools.cache
def _checked() -> None:
    try:
        torchsky = importlib.import_module("torchsky")
    except ImportError as exc:
        raise CrossMatchError(
            f"torchsky is installed but cannot be imported ({exc}); repair it with "
            f"{INSTALL_HINT} or uninstall it"
        ) from exc
    version = str(getattr(torchsky, "__version__", "") or "")
    match = re.match(r"(\d+)\.(\d+)\.(\d+)", version)
    found = tuple(int(part) for part in match.groups()) if match else None
    if found is None or found < MIN_VERSION:
        raise CrossMatchError(
            f"xmatcher needs torchsky>={_MIN_TEXT}, found {version or 'an unversioned copy'} "
            f"at {getattr(torchsky, '__file__', '?')}; upgrade with {INSTALL_HINT} "
            "or uninstall it"
        )


def module(name: str) -> ModuleType | None:
    """Import ``torchsky.<name>``; ``None`` when Torchsky is not installed."""
    if not installed():
        return None
    _checked()
    return importlib.import_module(f"torchsky.{name}")


def require(name: str, feature: str) -> ModuleType:
    """Import ``torchsky.<name>`` for ``feature``, which has no Torchsky-free path."""
    mod = module(name)
    if mod is None:
        raise CrossMatchError(f"{feature} requires torchsky>={_MIN_TEXT}: {INSTALL_HINT}")
    return mod

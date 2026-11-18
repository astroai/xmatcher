"""Optional Torchsky integration: one import point and one version floor.

Torchsky (``pip install 'xmatcher[torchsky]'``) provides the tensor-native
``engine="torchsky"`` matcher, the epoch-propagation Jacobians that target-epoch
``skyerr``/``skyellipse`` covariance propagation needs, and faster HEALPix
coverings.  Without it, proper motions are propagated with Astropy and HEALPix
coverings use cdshealpix.

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

MIN_VERSION = (0, 5, 0)
_MIN_TEXT = ".".join(str(part) for part in MIN_VERSION)
INSTALL_HINT = "pip install 'xmatcher[torchsky]'"


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

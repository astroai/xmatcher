"""Live ``TAP_SCHEMA`` check for every bundled catalogue entry.

Opt-in, because it queries the archives: run with ``XMATCHER_LIVE=1``.

This is the guard for the ``galex`` class of mistake: a bundled
``access_identifier`` that names the catalogue parent (`II/335`) instead of a
table (`II/335/galex_ais`), a misspelled coordinate column, or a `default_columns`
entry that no longer exists. Those all fail only when someone actually matches,
so the registry is otherwise validated by hand runs.
"""

from __future__ import annotations

import contextlib
import os
import re
import socket
import time

import pytest

from xmatcher.crossmatch import CrossMatch
from xmatcher.discovery import get_table_schema
from xmatcher.exceptions import ConfigError, TapError

# Archive outages are not registry defects: Data Lab returns 502 for every
# endpoint when it is down (observed 2026-10-22). Retry briefly, then skip
# with the reason instead of hiding a real schema drift behind an outage.
_TRANSIENT = re.compile(
    r"\b5\d\d\b|Bad Gateway|Service Unavailable|Gateway Timeout|timed out|"
    r"Connection (?:refused|reset)|Temporary failure|Remote end closed",
)
_MAX_ATTEMPTS = 3

LIVE = os.environ.get("XMATCHER_LIVE") == "1"

pytestmark = pytest.mark.skipif(
    not LIVE,
    reason="live TAP checks are opt-in: set XMATCHER_LIVE=1",
)


def _cases() -> list[tuple[str, object]]:
    """Configured catalogues that read from a TAP service, with their source."""
    cm = CrossMatch()
    cases: list[tuple[str, object]] = []
    for name in sorted(cm.catalogues_config):
        try:
            source = cm.resolve_source(name, {})
        except ConfigError:
            # Local HATS aliases require an environment variable that may be
            # unset on a laptop; they are not TAP catalogues anyway.
            continue
        if not source.tap_url or not source.access_identifier:
            continue
        if source.access_method not in ("tap", "cds_xmatch"):
            continue
        cases.append((name, source))
    return cases


CASES = _cases()


def _declared_columns(source) -> list[str]:
    """Every column the bundled entry promises, in one list."""
    columns = [
        source.ra_column,
        source.dec_column,
        source.id_column,
        source.ra_err_column,
        source.dec_err_column,
        source.corr_column,
        source.pm_ra_column,
        source.pm_dec_column,
        source.parallax_column,
        source.radial_velocity_column,
    ]
    columns.extend(source.default_columns or [])
    return [column for column in columns if column]


@contextlib.contextmanager
def _query_timeout(seconds: float = 90.0):
    """Bound one archive query so a stalled service cannot hang the run.

    PyVO/requests wait forever when the server accepts the connection and then
    stops answering; a socket-level default timeout turns that into a
    transient error the retry/skip logic can handle.
    """
    previous = socket.getdefaulttimeout()
    socket.setdefaulttimeout(seconds)
    try:
        yield
    finally:
        socket.setdefaulttimeout(previous)


@pytest.mark.parametrize("name,source", CASES, ids=[name for name, _ in CASES])
def _live_schema(name: str, source):
    last: TapError | None = None
    for attempt in range(_MAX_ATTEMPTS):
        try:
            with _query_timeout():
                return get_table_schema(source.tap_url, source.access_identifier)
        except TapError as exc:
            last = exc
            if not _TRANSIENT.search(str(exc)):
                raise
            time.sleep(1.5 * 2**attempt)
    pytest.skip(f"{name}: archive unavailable after {_MAX_ATTEMPTS} attempts ({last})")


@pytest.mark.parametrize("name,source", CASES, ids=[name for name, _ in CASES])
def test_bundled_catalogue_matches_its_live_tap_schema(name: str, source) -> None:
    schema = _live_schema(name, source)
    assert schema["columns_count"] > 0, (
        f"{name}: {source.access_identifier!r} is not a table on {source.tap_url} "
        f"(0 columns in TAP_SCHEMA)"
    )
    available = set(schema["columns_list"])
    missing = [column for column in _declared_columns(source) if column not in available]
    assert not missing, (
        f"{name}: {source.access_identifier!r} declares columns that the live "
        f"schema does not have: {missing}. Available: {sorted(available)[:20]}…"
    )

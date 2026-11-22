"""Retry timing shared by the HTTP, S3 and Hugging Face readers of the mirror."""

from __future__ import annotations

import email.utils
import time
from datetime import UTC

#: Delay used when a throttling answer carries no usable ``Retry-After``.
DEFAULT_DELAY_S = 1.0
#: Upper bound of one backoff sleep.
BACKOFF_CAP_S = 60.0


def retry_after_seconds(value: str | None, *, now: float | None = None) -> float:
    """Seconds to wait for a ``Retry-After`` header value.

    RFC 9110 allows delta-seconds (``"120"``) or an HTTP-date
    (``"Wed, 21 Oct 2026 07:28:00 GMT"``); archives behind CDNs send the
    latter. A date in the past means "now" (0 s). A missing or unparsable
    value gives :data:`DEFAULT_DELAY_S`.
    """
    if not value:
        return DEFAULT_DELAY_S
    text = value.strip()
    try:
        seconds = float(text)
    except ValueError:
        try:
            when = email.utils.parsedate_to_datetime(text)
        except (TypeError, ValueError):
            return DEFAULT_DELAY_S
        if when.tzinfo is None:
            when = when.replace(tzinfo=UTC)
        seconds = when.timestamp() - (time.time() if now is None else now)
    if seconds != seconds:  # NaN
        return DEFAULT_DELAY_S
    return max(0.0, seconds)


def backoff_sleep(delay: float, attempt: int) -> None:
    """Sleep ``delay * 2**attempt`` (+0.1 s), capped at :data:`BACKOFF_CAP_S`."""
    time.sleep(min(delay * (2**attempt) + 0.1, BACKOFF_CAP_S))

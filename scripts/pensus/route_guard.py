"""Reject forbidden match engines for pensus production paths."""

from __future__ import annotations

FORBIDDEN_ENGINES = frozenset({"auto", "ray-union", "ray"})
REQUIRED_ENGINE = "fast"
REQUIRED_FALLBACK = "error"


class PensusRouteError(ValueError):
    """Raised when a pensus job would use a forbidden routing choice."""


def guard_match_route(
    *,
    engine: str,
    fallback_policy: str,
    allow_oracle: bool = False,
) -> None:
    """Fail closed unless the production pensus route is used.

    Oracle harnesses may pass ``allow_oracle=True`` to permit ``stilts`` /
    ``lsdb`` comparison engines outside the production tile path.
    """
    eng = (engine or "").strip().lower()
    fb = (fallback_policy or "").strip().lower()
    if eng in FORBIDDEN_ENGINES:
        raise PensusRouteError(
            f"pensus forbids engine={engine!r}; use engine={REQUIRED_ENGINE!r} "
            f"with fallback_policy={REQUIRED_FALLBACK!r}"
        )
    if not allow_oracle and eng != REQUIRED_ENGINE:
        raise PensusRouteError(
            f"pensus production requires engine={REQUIRED_ENGINE!r}, got {engine!r}"
        )
    if fb != REQUIRED_FALLBACK:
        raise PensusRouteError(
            f"pensus requires fallback_policy={REQUIRED_FALLBACK!r}, got {fallback_policy!r}"
        )

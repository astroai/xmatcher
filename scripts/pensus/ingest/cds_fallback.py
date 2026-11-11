#!/usr/bin/env python3
"""CDS fallback trigger: if CDS still down N days before pilot, start CatWISE (+ GALEX/VHS)."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, date, datetime

DEFAULT_TRIGGER_DAYS = 7
SURVEYS = ("catwise2020", "galex_guvcat", "vhs_dr5")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pilot-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--cds-up", action="store_true")
    parser.add_argument("--days", type=int, default=DEFAULT_TRIGGER_DAYS)
    parser.add_argument("--today", default=None, help="override UTC today YYYY-MM-DD")
    args = parser.parse_args(argv)

    pilot = date.fromisoformat(args.pilot_date)
    today = date.fromisoformat(args.today) if args.today else datetime.now(UTC).date()
    days_left = (pilot - today).days
    fire = (not args.cds_up) and days_left <= args.days
    report = {
        "today": today.isoformat(),
        "pilot_date": pilot.isoformat(),
        "days_left": days_left,
        "cds_up": bool(args.cds_up),
        "trigger_days": args.days,
        "fire": fire,
        "surveys": list(SURVEYS) if fire else [],
        "action": (
            "start chunked builds into /arc/projects/k-pop/pensus/hats_build/{survey}"
            if fire
            else "no action (CDS up or outside trigger window)"
        ),
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if not fire else 10  # nonzero advertises "fallback active" to wrappers


if __name__ == "__main__":
    raise SystemExit(main())

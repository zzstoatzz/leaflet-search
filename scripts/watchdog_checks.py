"""Pure watchdog policy helpers, kept separate from network and auth code.

Snapshot adoption is windowed. `adoptWindowOpen` in backend/src/promote.zig
only lets the promote watcher adopt a new build during the UTC hours named by
PROMOTE_ADOPT_UTC_HOURS, because every adoption starts serving from a file with
zero warm pages -- on the 1GB app machine that meant a 10s+ cold common-word
search after every build (2026-08-16, commit ea2a72b). The overlay carries live
freshness between adoptions, so once a day off-peak is enough.

That makes raw snapshot age useless as an alarm signal: with a single daily
window, a snapshot legitimately reaches ~24h old just before the next adoption.
A duplicated 180m constant here (written for the old adopt-every-build
behavior) therefore paged ~21 hours out of every 24 once the window went daily.

The signal that actually means something is an adoption window closing WITHOUT
the serving snapshot advancing -- that is the builder or the promote watcher
being dead. So the bound below is derived from the same policy that governs
adoption, read from backend/fly.toml, instead of being restated as a number
that silently decouples the next time the window changes.

The policy is read from the repo rather than asked of the backend on purpose:
every check here lives outside the serving path's failure domain, and a wedged
backend must not get to declare its own freshness bound.
"""

from __future__ import annotations

import datetime
from pathlib import Path
from typing import NamedTuple

import tomllib

FLY_TOML = Path(__file__).resolve().parents[1] / "backend" / "fly.toml"

# Slack for "the build adopted during a window may predate that window's start".
# The builder runs on its own cadence (2h heavypad cron), so the newest build
# sitting in R2 when a window opens can be nearly one cadence old, plus the time
# the build itself took. This is tolerance around the window-derived bound, not
# the bound -- cadence drift widens or narrows the margin, it can no longer
# invert the check the way the old standalone constant did.
ADOPTION_LAG_GRACE_MINUTES = 150

# Bound used when adoption is unwindowed (PROMOTE_ADOPT_UTC_HOURS unset or
# empty = adopt on discovery, which is the dev and pre-window behavior). Then
# the snapshot really should track the build cadence: 2h cadence + ~20m build
# + 5m adoption poll + margin.
UNWINDOWED_SNAPSHOT_AGE_ALERT_MINUTES = 180

_HOURS_SCANNED_BACK = 24 * 8


class AdoptionPolicy(NamedTuple):
    """What backend/fly.toml says about adopting new snapshots.

    hours is None when adoption is unwindowed (adopt on discovery). invalid
    holds tokens promote.zig's parseInt-and-skip loop will silently drop; a
    policy whose every token is invalid can never open its window, which
    freezes adoption forever and is worth paging for on its own.
    """

    enabled: bool
    hours: tuple[int, ...] | None
    invalid: tuple[str, ...]


def parse_adopt_utc_hours(raw: str | None) -> tuple[tuple[int, ...] | None, tuple[str, ...]]:
    """Mirror hoursListContains in backend/src/promote.zig.

    Unset or empty means the window is always open. Anything else is a
    comma-separated hour list; tokens zig's `parseInt ... catch continue` would
    drop are reported separately instead of being silently ignored here too.
    """
    if raw is None or not raw.strip():
        return None, ()
    hours: list[int] = []
    invalid: list[str] = []
    for token in raw.split(","):
        stripped = token.strip()
        try:
            hour = int(stripped)
        except ValueError:
            invalid.append(stripped)
            continue
        if 0 <= hour <= 23:
            hours.append(hour)
        else:
            invalid.append(stripped)
    return tuple(sorted(set(hours))), tuple(invalid)


def read_adoption_policy(fly_toml: Path | None = None) -> AdoptionPolicy:
    """Read the deployed adoption policy from backend/fly.toml's [env] block."""
    env = tomllib.loads((fly_toml or FLY_TOML).read_text()).get("env", {})
    hours, invalid = parse_adopt_utc_hours(env.get("PROMOTE_ADOPT_UTC_HOURS"))
    return AdoptionPolicy(
        enabled=str(env.get("ENABLE_SNAPSHOT_PROMOTE", "")) == "1",
        hours=hours,
        invalid=invalid,
    )


def last_closed_adoption_window(
    now: datetime.datetime, hours: tuple[int, ...]
) -> datetime.datetime | None:
    """Start of the most recent adoption window that has fully elapsed.

    The window currently in progress is deliberately excluded: adoption is
    allowed to happen at any point within its hour, so demanding a fresh
    snapshot mid-window would page on a race the watcher is entitled to win.
    """
    wanted = set(hours)
    cursor = now.replace(minute=0, second=0, microsecond=0) - datetime.timedelta(hours=1)
    for _ in range(_HOURS_SCANNED_BACK):
        if cursor.hour in wanted:
            return cursor
        cursor -= datetime.timedelta(hours=1)
    return None


def snapshot_age_minutes(manifest: object, now_timestamp: float) -> float:
    if not isinstance(manifest, dict):
        raise ValueError("snapshot manifest body is not an object")
    created_at = manifest.get("created_at")
    if not isinstance(created_at, (int, float)) or created_at <= 0:
        raise ValueError("snapshot manifest has invalid created_at")
    return (now_timestamp - created_at) / 60


def snapshot_age_problem(
    manifest: object, now_timestamp: float, policy: AdoptionPolicy
) -> str | None:
    """Describe why the serving snapshot is unacceptably stale, or None.

    Staleness is judged against the adoption policy, not the clock: the
    question is whether an adoption opportunity has come and gone while the
    serving build stayed put.
    """
    if not isinstance(manifest, dict):
        return "snapshot manifest body is not an object"

    build_id = manifest.get("build_id", "?")
    created_at = manifest.get("created_at")
    if not isinstance(created_at, (int, float)) or created_at <= 0:
        return f"snapshot manifest has invalid created_at (build {build_id})"

    age_minutes = snapshot_age_minutes(manifest, now_timestamp)

    if policy.invalid:
        return (
            f"PROMOTE_ADOPT_UTC_HOURS names unusable hour(s) {','.join(policy.invalid)} — "
            "promote.zig skips tokens it cannot parse, so the adoption window may never "
            f"open (serving build {build_id}, {age_minutes:.0f}m old)"
        )

    if not policy.enabled:
        # ENABLE_SNAPSHOT_PROMOTE is off: nothing is expected to adopt, so age
        # carries no signal. The freshness and serving checks still apply.
        return None

    if policy.hours is None:
        if age_minutes > UNWINDOWED_SNAPSHOT_AGE_ALERT_MINUTES:
            return (
                f"serving snapshot is {age_minutes:.0f}m old (build {build_id}) - adoption "
                "is unwindowed and should track the build cadence; builder or adoption delayed"
            )
        return None

    now = datetime.datetime.fromtimestamp(now_timestamp, datetime.timezone.utc)
    window_start = last_closed_adoption_window(now, policy.hours)
    if window_start is None:
        return None

    deadline = window_start.timestamp() - ADOPTION_LAG_GRACE_MINUTES * 60
    if created_at < deadline:
        return (
            f"the {window_start:%Y-%m-%dT%H:00Z} adoption window closed without adopting a "
            f"newer build - still serving {build_id} ({age_minutes:.0f}m old); builder or "
            "promote watcher delayed"
        )
    return None

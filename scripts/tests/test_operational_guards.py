import datetime
import sys
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

from watchdog_checks import (  # noqa: E402
    UNWINDOWED_SNAPSHOT_AGE_ALERT_MINUTES,
    AdoptionPolicy,
    last_closed_adoption_window,
    parse_adopt_utc_hours,
    read_adoption_policy,
    snapshot_age_minutes,
    snapshot_age_problem,
)

DAILY_0800 = AdoptionPolicy(enabled=True, hours=(8,), invalid=())
UNWINDOWED = AdoptionPolicy(enabled=True, hours=None, invalid=())


def ts(iso: str) -> float:
    return datetime.datetime.fromisoformat(iso).replace(tzinfo=datetime.timezone.utc).timestamp()


class AdoptionPolicyParsingTests(unittest.TestCase):
    def test_unset_and_empty_mean_unwindowed(self):
        self.assertEqual((None, ()), parse_adopt_utc_hours(None))
        self.assertEqual((None, ()), parse_adopt_utc_hours(""))
        self.assertEqual((None, ()), parse_adopt_utc_hours("   "))

    def test_single_and_multi_hour_lists(self):
        self.assertEqual(((8,), ()), parse_adopt_utc_hours("8"))
        self.assertEqual(((8, 20), ()), parse_adopt_utc_hours(" 20, 8 "))
        self.assertEqual(((8,), ()), parse_adopt_utc_hours("8,8"))

    def test_unusable_tokens_are_reported_not_swallowed(self):
        # promote.zig does `parseInt ... catch continue`, so these vanish there.
        self.assertEqual(((8,), ("noon",)), parse_adopt_utc_hours("8,noon"))
        self.assertEqual(((), ("25",)), parse_adopt_utc_hours("25"))

    def test_reads_the_deployed_policy_from_fly_toml(self):
        # the point of the whole design: one source of truth, not a copy.
        policy = read_adoption_policy()
        self.assertTrue(policy.enabled)
        self.assertEqual((), policy.invalid)
        self.assertIsNotNone(policy.hours)


class AdoptionWindowTests(unittest.TestCase):
    def test_window_in_progress_is_not_yet_due(self):
        now = datetime.datetime(2026, 8, 17, 8, 30, tzinfo=datetime.timezone.utc)
        self.assertEqual(
            datetime.datetime(2026, 8, 16, 8, tzinfo=datetime.timezone.utc),
            last_closed_adoption_window(now, (8,)),
        )

    def test_window_just_closed_becomes_due(self):
        now = datetime.datetime(2026, 8, 17, 9, 1, tzinfo=datetime.timezone.utc)
        self.assertEqual(
            datetime.datetime(2026, 8, 17, 8, tzinfo=datetime.timezone.utc),
            last_closed_adoption_window(now, (8,)),
        )

    def test_picks_the_nearest_of_several_windows(self):
        now = datetime.datetime(2026, 8, 17, 15, 0, tzinfo=datetime.timezone.utc)
        self.assertEqual(
            datetime.datetime(2026, 8, 17, 14, tzinfo=datetime.timezone.utc),
            last_closed_adoption_window(now, (8, 14, 20)),
        )


class WindowedSnapshotAgeTests(unittest.TestCase):
    def test_daily_window_tolerates_a_nearly_day_old_snapshot(self):
        # regression, 2026-08-17: adoption went daily (PROMOTE_ADOPT_UTC_HOURS=8,
        # commit ea2a72b) while the watchdog kept a standalone 180m bound, so it
        # failed every run from ~11:40Z until the next morning — ~21h/day.
        manifest = {"build_id": "b1786956116-a1f5", "created_at": ts("2026-08-17T08:41:56")}
        for hour in (12, 15, 20, 23):
            with self.subTest(hour=hour):
                self.assertIsNone(
                    snapshot_age_problem(manifest, ts(f"2026-08-17T{hour}:30:00"), DAILY_0800)
                )
        # still fine right up against the next window opening
        self.assertIsNone(
            snapshot_age_problem(manifest, ts("2026-08-18T07:59:00"), DAILY_0800)
        )

    def test_missed_window_pages_within_the_hour(self):
        manifest = {"build_id": "b-yesterday", "created_at": ts("2026-08-17T08:41:56")}
        problem = snapshot_age_problem(manifest, ts("2026-08-18T09:05:00"), DAILY_0800)
        self.assertIsNotNone(problem)
        self.assertIn("2026-08-18T08:00Z", problem)
        self.assertIn("b-yesterday", problem)
        self.assertIn("adoption window closed", problem)

    def test_build_predating_the_window_start_is_tolerated(self):
        # the newest build in R2 when a window opens can be most of one builder
        # cadence old; that is lag, not a dead adopter.
        manifest = {"build_id": "b-prior-cadence", "created_at": ts("2026-08-18T06:41:00")}
        self.assertIsNone(
            snapshot_age_problem(manifest, ts("2026-08-18T09:05:00"), DAILY_0800)
        )

    def test_build_older_than_the_grace_is_not_tolerated(self):
        manifest = {"build_id": "b-too-old", "created_at": ts("2026-08-18T05:00:00")}
        self.assertIsNotNone(
            snapshot_age_problem(manifest, ts("2026-08-18T09:05:00"), DAILY_0800)
        )


class UnwindowedSnapshotAgeTests(unittest.TestCase):
    def test_threshold_covers_offbox_cadence(self):
        # 2h heavypad cadence + ~20m build + 5m adoption poll + margin
        self.assertEqual(180, UNWINDOWED_SNAPSHOT_AGE_ALERT_MINUTES)

    def test_at_threshold_is_healthy(self):
        now = 100_000.0
        self.assertIsNone(
            snapshot_age_problem({"build_id": "b1", "created_at": now - 180 * 60}, now, UNWINDOWED)
        )

    def test_past_threshold_names_build(self):
        now = 100_000.0
        problem = snapshot_age_problem(
            {"build_id": "b-stale", "created_at": now - 181 * 60}, now, UNWINDOWED
        )
        self.assertIn("181m old", problem)
        self.assertIn("b-stale", problem)
        self.assertIn("adoption delayed", problem)


class PolicyEdgeTests(unittest.TestCase):
    def test_promote_disabled_means_age_carries_no_signal(self):
        disabled = AdoptionPolicy(enabled=False, hours=(8,), invalid=())
        manifest = {"build_id": "b-ancient", "created_at": ts("2026-01-01T00:00:00")}
        self.assertIsNone(snapshot_age_problem(manifest, ts("2026-08-17T15:00:00"), disabled))

    def test_unusable_window_config_pages(self):
        broken = AdoptionPolicy(enabled=True, hours=(), invalid=("25",))
        problem = snapshot_age_problem(
            {"build_id": "b1", "created_at": ts("2026-08-17T08:00:00")},
            ts("2026-08-17T09:00:00"),
            broken,
        )
        self.assertIn("25", problem)
        self.assertIn("never", problem)

    def test_invalid_manifest_fails_closed(self):
        self.assertIn(
            "invalid created_at",
            snapshot_age_problem({"build_id": "b1"}, 10_000.0, DAILY_0800),
        )
        self.assertIn("not an object", snapshot_age_problem("nope", 10_000.0, DAILY_0800))

    def test_age_helper_returns_minutes(self):
        self.assertEqual(2.0, snapshot_age_minutes({"created_at": 880.0}, 1_000.0))


if __name__ == "__main__":
    unittest.main()

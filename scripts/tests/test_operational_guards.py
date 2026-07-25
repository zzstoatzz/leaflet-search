import sys
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

from watchdog_checks import SNAPSHOT_AGE_ALERT_MINUTES, snapshot_age_minutes, snapshot_age_problem  # noqa: E402


class SnapshotAgeTests(unittest.TestCase):
    def test_threshold_covers_offbox_cadence(self):
        # 2h heavypad cadence + ~20m build + 5m adoption poll + margin
        self.assertEqual(180, SNAPSHOT_AGE_ALERT_MINUTES)

    def test_at_threshold_is_healthy(self):
        now = 100_000.0
        self.assertIsNone(snapshot_age_problem({"build_id": "b1", "created_at": now - 180 * 60}, now))

    def test_past_threshold_names_build(self):
        now = 100_000.0
        problem = snapshot_age_problem({"build_id": "b-stale", "created_at": now - 181 * 60}, now)
        self.assertIn("181m old", problem)
        self.assertIn("b-stale", problem)
        self.assertIn("freshness bound exceeded", problem)

    def test_invalid_manifest_fails_closed(self):
        self.assertIn("invalid created_at", snapshot_age_problem({"build_id": "b1"}, 10_000.0))

    def test_age_helper_returns_minutes(self):
        self.assertEqual(2.0, snapshot_age_minutes({"created_at": 880.0}, 1_000.0))



if __name__ == "__main__":
    unittest.main()

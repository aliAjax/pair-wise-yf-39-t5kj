import unittest

from src.rules import is_cluster
from src.domain import Actor, PermissionDenied, ValidationError
from src.rules import RuleEngine


class RulesTest(unittest.TestCase):
    def setUp(self):
        self.rules = RuleEngine()
        self.admin = Actor("rule-tester", "admin")

    def test_rule_calculation_or_validation(self):
        points = [
            {"id": "1", "observed_at": "2026-01-01", "lat": 30.0, "lon": 120.0},
            {"id": "2", "observed_at": "2026-01-02", "lat": 30.01, "lon": 120.01},
            {"id": "3", "observed_at": "2026-01-03", "lat": 30.02, "lon": 120.02},
        ]
        self.assertTrue(is_cluster(points))
        points[2]["lon"] = 130.0
        self.assertFalse(is_cluster(points))


if __name__ == "__main__":
    unittest.main()

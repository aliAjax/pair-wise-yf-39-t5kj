import tempfile
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError, PermissionDenied
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class FailureTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = SQLiteRepository(Path(self.tmp.name) / "test.db")
        self.service = DomainService(self.repo, RuleEngine())

    def tearDown(self):
        self.tmp.cleanup()

    def test_permission_denied(self):
        entity = self.service.create(
            Actor("admin", "admin"), 'observation', {'event_id': 'E-9', 'species': 'deer', 'location': 'N', 'observed_at': '2026-01-01', 'lat': 1.0, 'lon': 1.0}
        )
        with self.assertRaises(PermissionDenied):
            self.service.transition(
                Actor("viewer", "viewer"),
                entity["id"],
                'submit',
                {'location': 'N', 'observed_at': '2026-01-01'},
            )

    def test_version_conflict(self):
        entity = self.service.create(
            Actor("admin", "admin"), 'observation', {'event_id': 'E-9', 'species': 'deer', 'location': 'N', 'observed_at': '2026-01-01', 'lat': 1.0, 'lon': 1.0}
        )
        with self.assertRaises(ConflictError):
            self.service.transition(
                Actor("admin", "admin"),
                entity["id"],
                'submit',
                {'location': 'N', 'observed_at': '2026-01-01'},
                expected_version=999,
            )

    def test_duplicate_idempotency_key_returns_same_entity(self):
        first = self.service.create(
            Actor("admin", "admin"),
            'observation',
            {'event_id': 'E-9', 'species': 'deer', 'location': 'N', 'observed_at': '2026-01-01', 'lat': 1.0, 'lon': 1.0},
            idempotency_key="duplicate-check",
        )
        second = self.service.create(
            Actor("admin", "admin"),
            'observation',
            {'event_id': 'E-9', 'species': 'deer', 'location': 'N', 'observed_at': '2026-01-01', 'lat': 1.0, 'lon': 1.0},
            idempotency_key="duplicate-check",
        )
        self.assertEqual(first["id"], second["id"])


if __name__ == "__main__":
    unittest.main()

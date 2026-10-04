import tempfile
import unittest
from pathlib import Path

from src.domain import Actor
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


def _resolve(value, created):
    if isinstance(value, str):
        for key, item in created.items():
            value = value.replace("{" + key + "}", str(item))
        return value
    if isinstance(value, list):
        return [_resolve(item, created) for item in value]
    if isinstance(value, dict):
        return {key: _resolve(item, created) for key, item in value.items()}
    return value


class WorkflowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = SQLiteRepository(Path(self.tmp.name) / "test.db")
        self.service = DomainService(self.repo, RuleEngine())
        self.actor = Actor("admin", "admin")

    def tearDown(self):
        self.tmp.cleanup()

    def test_full_workflow(self):
        created = {}
        steps = [{'op': 'create', 'as': 'o1', 'kind': 'observation', 'data': {'event_id': 'E-1', 'species': 'deer', 'location': 'North', 'observed_at': '2026-04-01', 'lat': 40.0, 'lon': 116.0}}, {'op': 'transition', 'target': 'o1', 'action': 'submit', 'data': {'location': 'North', 'observed_at': '2026-04-01'}, 'expect': 'submitted'}, {'op': 'create', 'as': 'o2', 'kind': 'observation', 'data': {'event_id': 'E-2', 'species': 'deer', 'location': 'North', 'observed_at': '2026-04-03', 'lat': 40.01, 'lon': 116.01}}, {'op': 'transition', 'target': 'o2', 'action': 'submit', 'data': {'location': 'North', 'observed_at': '2026-04-03'}, 'expect': 'submitted'}, {'op': 'create', 'as': 'o3', 'kind': 'observation', 'data': {'event_id': 'E-3', 'species': 'deer', 'location': 'North', 'observed_at': '2026-04-05', 'lat': 40.02, 'lon': 116.02}}, {'op': 'transition', 'target': 'o3', 'action': 'submit', 'data': {'location': 'North', 'observed_at': '2026-04-05'}, 'expect': 'submitted'}, {'op': 'create', 'as': 'sample', 'kind': 'sample', 'data': {'observation_id': '{o1}', 'sample_code': 'W-1'}}, {'op': 'transition', 'target': 'sample', 'action': 'send_lab', 'data': {'lab_id': 'LAB-1'}, 'expect': 'in_lab'}, {'op': 'transition', 'target': 'sample', 'action': 'lab_result', 'data': {'result': 'positive', 'result_at': '2026-04-02'}, 'expect': 'resulted'}, {'op': 'create', 'as': 'cluster', 'kind': 'cluster', 'data': {'region': 'North'}}, {'op': 'transition', 'target': 'cluster', 'action': 'confirm_cluster', 'data': {'observation_ids': ['{o1}', '{o2}', '{o3}'], 'centroid': [40.01, 116.01]}, 'expect': 'confirmed'}]
        for step in steps:
            if step["op"] == "create":
                entity = self.service.create(
                    self.actor,
                    step["kind"],
                    _resolve(step.get("data", {}), created),
                    step.get("idempotency_key"),
                )
                created[step["as"]] = entity["id"]
            else:
                entity = self.service.transition(
                    self.actor,
                    created[step["target"]],
                    step["action"],
                    _resolve(step.get("data", {}), created),
                    step.get("expected_version"),
                )
            if "expect" in step:
                self.assertEqual(entity["status"], step["expect"])


if __name__ == "__main__":
    unittest.main()

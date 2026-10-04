import tempfile
import unittest
from pathlib import Path

from src.domain import Actor
from src.offline import (
    OfflineConflictError,
    OfflineService,
    STATUS_CONFIRMED,
    STATUS_CONFLICT,
)
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


def _obs(event_id, species="deer", location="North", observed_at="2026-04-01",
         lat=40.0, lon=116.0):
    return {
        "event_id": event_id,
        "species": species,
        "location": location,
        "observed_at": observed_at,
        "lat": lat,
        "lon": lon,
    }


class OfflineBatchTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = SQLiteRepository(Path(self.tmp.name) / "test.db")
        self.rules = RuleEngine()
        self.service = DomainService(self.repo, self.rules)
        self.offline = OfflineService(self.repo, self.rules)
        self.actor = Actor("admin", "admin")

    def tearDown(self):
        self.tmp.cleanup()

    def _item(self, item_id, op, **kwargs):
        item = {"item_id": item_id, "op": op}
        item.update(kwargs)
        return item

    def test_batch_creates_entities(self):
        batch = self.offline.submit_batch(
            self.actor,
            "batch-1",
            [
                self._item("obs-1", "create", kind="observation", data=_obs("E-1")),
                self._item("obs-1-submit", "transition", entity_ref="@obs-1",
                            action="submit", data={"location": "North", "observed_at": "2026-04-01"}),
                self._item("samp-1", "create", kind="sample",
                            data={"observation_id": "@obs-1", "sample_code": "W-1"}),
                self._item("samp-1-send", "transition", entity_ref="@samp-1",
                            action="send_lab", data={"lab_id": "LAB-1"}),
            ],
            device_id="device-a",
        )
        self.assertEqual(batch["status"], "completed")
        statuses = {item["item_id"]: item["status"] for item in batch["items"]}
        self.assertEqual(set(statuses.values()), {STATUS_CONFIRMED})
        self.assertEqual(len(batch["items"]), 4)

    def test_duplicate_resubmission_creates_no_duplicates(self):
        items = [
            self._item("obs-1", "create", kind="observation", data=_obs("E-1")),
            self._item("obs-1-submit", "transition", entity_ref="@obs-1",
                        action="submit", data={"location": "North", "observed_at": "2026-04-01"}),
            self._item("samp-1", "create", kind="sample",
                        data={"observation_id": "@obs-1", "sample_code": "W-1"}),
        ]
        first = self.offline.submit_batch(self.actor, "batch-dup", items)
        first_obs = first["items"][0]["result"]["entity_id"]
        first_samp = first["items"][2]["result"]["entity_id"]

        audit_before = self.repo.count_audit()
        entities_before = len(self.service.list())

        # Retry the exact same batch after a network outage.
        second = self.offline.submit_batch(self.actor, "batch-dup", items)
        second_obs = second["items"][0]["result"]["entity_id"]
        second_samp = second["items"][2]["result"]["entity_id"]

        self.assertEqual(first_obs, second_obs)
        self.assertEqual(first_samp, second_samp)
        self.assertEqual(self.repo.count_audit(), audit_before)
        self.assertEqual(len(self.service.list()), entities_before)

    def test_resume_from_confirmed_point(self):
        items = [
            self._item("obs-1", "create", kind="observation", data=_obs("E-1")),
            self._item("obs-2", "create", kind="observation", data=_obs("E-2", observed_at="2026-04-03", lat=40.01, lon=116.01)),
            self._item("obs-3", "create", kind="observation", data=_obs("E-3", observed_at="2026-04-05", lat=40.02, lon=116.02)),
        ]
        self.offline.submit_batch(self.actor, "batch-resume", items)

        # Simulate a retry that also adds a new item.
        retry = items + [
            self._item("obs-4", "create", kind="observation", data=_obs("E-4", observed_at="2026-04-07", lat=40.03, lon=116.03)),
        ]
        batch = self.offline.submit_batch(self.actor, "batch-resume", retry)
        self.assertEqual(batch["status"], "completed")
        self.assertEqual(len(batch["items"]), 4)
        # The first three items keep their original results (not reprocessed).
        ids = [item["result"]["entity_id"] for item in batch["items"]]
        self.assertEqual(len(set(ids)), 4)

    def test_restart_resilience(self):
        items = [
            self._item("obs-1", "create", kind="observation", data=_obs("E-1")),
            self._item("obs-1-submit", "transition", entity_ref="@obs-1",
                        action="submit", data={"location": "North", "observed_at": "2026-04-01"}),
        ]
        self.offline.submit_batch(self.actor, "batch-restart", items)

        # Simulate a server restart: build a brand new offline service over the
        # same SQLite database and continue the same batch.
        restarted = OfflineService(self.repo, self.rules)
        batch = restarted.submit_batch(self.actor, "batch-restart", items)
        self.assertEqual(batch["status"], "completed")
        self.assertEqual(len(batch["items"]), 2)
        self.assertEqual(self.repo.count_audit(), 2)  # create + submit, no dupes

    def test_conflict_when_lab_result_finalised(self):
        # Batch 1: register observation, submit it, sample it, and produce a
        # positive lab result while the device is offline.
        setup = [
            self._item("obs-1", "create", kind="observation", data=_obs("E-1")),
            self._item("obs-1-submit", "transition", entity_ref="@obs-1",
                        action="submit", data={"location": "North", "observed_at": "2026-04-01"}),
            self._item("samp-1", "create", kind="sample",
                        data={"observation_id": "@obs-1", "sample_code": "W-1"}),
            self._item("samp-1-send", "transition", entity_ref="@samp-1",
                        action="send_lab", data={"lab_id": "LAB-1"}),
            self._item("samp-1-result", "transition", entity_ref="@samp-1",
                        action="lab_result", data={"result": "positive", "result_at": "2026-04-02"}),
        ]
        self.offline.submit_batch(self.actor, "batch-setup", setup)

        # Batch 2: the device retries with a stale correction to the location.
        correction = [
            self._item("obs-1-correction", "create", kind="observation",
                        data=_obs("E-1", location="South")),
        ]
        batch = self.offline.submit_batch(self.actor, "batch-correction", correction)
        item = batch["items"][0]
        self.assertEqual(item["status"], STATUS_CONFLICT)
        self.assertEqual(item["error"]["type"], "OfflineConflictError")
        details = item["error"]["details"]
        self.assertEqual(details["submitted"]["location"], "South")
        self.assertEqual(details["current"]["location"], "North")
        self.assertTrue(details["blocking_samples"])

        # The downstream conclusion is not overwritten.
        obs = self.service.get(details["observation_id"])
        self.assertEqual(obs["data"]["location"], "North")

    def test_conflict_when_cluster_confirmed(self):
        # Three observations close in time and space.
        setup = [
            self._item("obs-1", "create", kind="observation", data=_obs("E-1")),
            self._item("obs-1-submit", "transition", entity_ref="@obs-1",
                        action="submit", data={"location": "North", "observed_at": "2026-04-01"}),
            self._item("obs-2", "create", kind="observation",
                        data=_obs("E-2", observed_at="2026-04-03", lat=40.01, lon=116.01)),
            self._item("obs-2-submit", "transition", entity_ref="@obs-2",
                        action="submit", data={"location": "North", "observed_at": "2026-04-03"}),
            self._item("obs-3", "create", kind="observation",
                        data=_obs("E-3", observed_at="2026-04-05", lat=40.02, lon=116.02)),
            self._item("obs-3-submit", "transition", entity_ref="@obs-3",
                        action="submit", data={"location": "North", "observed_at": "2026-04-05"}),
        ]
        self.offline.submit_batch(self.actor, "batch-obs", setup)

        obs1 = self._entity_id("batch-obs", "obs-1")
        obs2 = self._entity_id("batch-obs", "obs-2")
        obs3 = self._entity_id("batch-obs", "obs-3")

        # Confirm a cluster server-side while the device is offline.
        cluster = self.service.create(
            self.actor, "cluster", {"region": "North"}
        )
        self.service.transition(
            self.actor, cluster["id"], "confirm_cluster",
            {"observation_ids": [obs1, obs2, obs3], "centroid": [40.01, 116.01]},
        )

        # The device retries with a stale correction to obs-1's location.
        correction = [
            self._item("obs-1-correction", "create", kind="observation",
                        data=_obs("E-1", location="South")),
        ]
        batch = self.offline.submit_batch(self.actor, "batch-cluster-correction", correction)
        item = batch["items"][0]
        self.assertEqual(item["status"], STATUS_CONFLICT)
        self.assertTrue(item["error"]["details"]["blocking_clusters"])

        # The cluster is untouched.
        cluster_after = self.service.get(cluster["id"])
        self.assertEqual(cluster_after["status"], "confirmed")
        obs1_after = self.service.get(obs1)
        self.assertEqual(obs1_after["data"]["location"], "North")

    def test_invalidation_recompute_and_audit_old_values(self):
        # Observation + sample in the lab (no result yet) -- downstream is not
        # finalised, so a core-field change should invalidate and recompute.
        setup = [
            self._item("obs-1", "create", kind="observation", data=_obs("E-1")),
            self._item("obs-1-submit", "transition", entity_ref="@obs-1",
                        action="submit", data={"location": "North", "observed_at": "2026-04-01"}),
            self._item("samp-1", "create", kind="sample",
                        data={"observation_id": "@obs-1", "sample_code": "W-1"}),
            self._item("samp-1-send", "transition", entity_ref="@samp-1",
                        action="send_lab", data={"lab_id": "LAB-1"}),
        ]
        self.offline.submit_batch(self.actor, "batch-inv", setup)

        # Device corrects the location; no finalised downstream yet.
        correction = [
            self._item("obs-1-correction", "create", kind="observation",
                        data=_obs("E-1", location="South")),
        ]
        batch = self.offline.submit_batch(self.actor, "batch-inv-correction", correction)
        item = batch["items"][0]
        self.assertEqual(item["status"], STATUS_CONFIRMED)

        obs_id = item["result"]["entity_id"]
        obs = self.service.get(obs_id)
        self.assertEqual(obs["data"]["location"], "South")

        # The sample was queued for recomputation.
        pending = self.repo.list_pending_recompute()
        self.assertTrue(any(p["entity_id"] == self._entity_id("batch-inv", "samp-1") for p in pending))

        # Recompute and verify the sample is re-evaluated.
        results = self.offline.recompute_pending(self.actor)
        self.assertTrue(results)
        samp = self.service.get(self._entity_id("batch-inv", "samp-1"))
        self.assertIn("recomputed_at", samp["data"])

        # Old values are kept in the audit trail.
        audit = self.repo.list_audit(obs_id)
        change_entries = [a for a in audit if a["action"] == "offline_change"]
        self.assertTrue(change_entries)
        self.assertEqual(change_entries[0]["detail"]["old"]["location"], "North")
        self.assertEqual(change_entries[0]["detail"]["new"]["location"], "South")

    def test_sample_change_queues_cluster_recompute(self):
        # Three observations forming a cluster, plus a sample on obs-1.
        setup = [
            self._item("obs-1", "create", kind="observation", data=_obs("E-1")),
            self._item("obs-1-submit", "transition", entity_ref="@obs-1",
                        action="submit", data={"location": "North", "observed_at": "2026-04-01"}),
            self._item("obs-2", "create", kind="observation",
                        data=_obs("E-2", observed_at="2026-04-03", lat=40.01, lon=116.01)),
            self._item("obs-2-submit", "transition", entity_ref="@obs-2",
                        action="submit", data={"location": "North", "observed_at": "2026-04-03"}),
            self._item("obs-3", "create", kind="observation",
                        data=_obs("E-3", observed_at="2026-04-05", lat=40.02, lon=116.02)),
            self._item("obs-3-submit", "transition", entity_ref="@obs-3",
                        action="submit", data={"location": "North", "observed_at": "2026-04-05"}),
            self._item("samp-1", "create", kind="sample",
                        data={"observation_id": "@obs-1", "sample_code": "W-1"}),
            self._item("samp-1-send", "transition", entity_ref="@samp-1",
                        action="send_lab", data={"lab_id": "LAB-1"}),
        ]
        self.offline.submit_batch(self.actor, "batch-sample-inv", setup)

        obs1 = self._entity_id("batch-sample-inv", "obs-1")
        obs2 = self._entity_id("batch-sample-inv", "obs-2")
        obs3 = self._entity_id("batch-sample-inv", "obs-3")
        samp1 = self._entity_id("batch-sample-inv", "samp-1")

        cluster = self.service.create(self.actor, "cluster", {"region": "North"})
        self.service.transition(
            self.actor, cluster["id"], "confirm_cluster",
            {"observation_ids": [obs1, obs2, obs3], "centroid": [40.01, 116.01]},
        )

        # A lab result is produced via the offline batch -- this is a sample
        # change that should invalidate the cluster for recomputation.  The
        # sample lives in a previous batch, so reference it by its entity id.
        result_batch = [
            self._item("samp-1-result", "transition", entity_ref=samp1,
                        action="lab_result", data={"result": "positive", "result_at": "2026-04-02"}),
        ]
        self.offline.submit_batch(self.actor, "batch-sample-result", result_batch)

        pending = self.repo.list_pending_recompute()
        self.assertTrue(
            any(p["entity_id"] == cluster["id"] and p["reason"] == "sample_changed"
                for p in pending)
        )

    def test_conflict_error_carries_details(self):
        err = OfflineConflictError("stale", {"submitted": {"location": "South"}})
        self.assertEqual(err.details["submitted"]["location"], "South")

    def _entity_id(self, batch_id, item_id):
        item = self.repo.get_offline_batch_item(batch_id, item_id)
        return item["result"]["entity_id"]


if __name__ == "__main__":
    unittest.main()

import tempfile
import unittest
from pathlib import Path

from src.domain import Actor
from src.offline import OfflineBatchService
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


def observation(event_id, observed_at="2026-04-01", lat=40.0, lon=116.0,
                location="North", species="deer", obs_id=None):
    data = {
        "event_id": event_id, "species": species, "location": location,
        "observed_at": observed_at, "lat": lat, "lon": lon,
    }
    entry = {"op": "create", "kind": "observation", "data": data}
    if obs_id:
        entry["id"] = obs_id
    return entry


class OfflineBatchTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "offline.db")
        self.repo = SQLiteRepository(self.db_path)
        self.rules = RuleEngine()
        self.service = DomainService(self.repo, self.rules)
        self.offline = OfflineBatchService(self.repo, self.rules)
        self.actor = Actor("ranger-1", "field")
        self.admin = Actor("admin", "admin")
        self.lab = Actor("lab-1", "lab")
        self.epi = Actor("epi-1", "epidemiologist")

    def tearDown(self):
        self.tmp.cleanup()

    def _fresh_services(self):
        """Simulate a station server restart against the same database."""
        repo = SQLiteRepository(self.db_path)
        return repo, DomainService(repo, self.rules), OfflineBatchService(repo, self.rules)

    # ------------------------------------------------------------------
    # Basic happy path
    # ------------------------------------------------------------------
    def test_batch_uploads_observations_and_sample_in_order(self):
        entries = [
            {"seq": 1, **observation("E-1", obs_id="obs-1")},
            {"seq": 2, **observation("E-2", observed_at="2026-04-03", lat=40.01, lon=116.01, obs_id="obs-2")},
            {"seq": 3, "op": "action", "entity_id": "obs-1", "action": "submit",
             "data": {"location": "North", "observed_at": "2026-04-01"}},
            {"seq": 4, "op": "create", "kind": "sample",
             "data": {"observation_id": "obs-1", "sample_code": "W-1"}, "id": "smp-1"},
        ]
        result = self.offline.submit(self.actor, "B-1", "device-7", entries)
        self.assertEqual(result["confirmed_seq"], 4)
        self.assertEqual(result["next_seq"], 5)
        statuses = [entry["status"] for entry in result["entries"]]
        self.assertEqual(statuses, ["applied"] * 4)

        sample = self.repo.get_entity("smp-1")
        self.assertEqual(sample["kind"], "sample")
        self.assertEqual(sample["status"], "collected")
        obs = self.repo.get_entity("obs-1")
        self.assertEqual(obs["status"], "submitted")

    # ------------------------------------------------------------------
    # Idempotent retries
    # ------------------------------------------------------------------
    def test_retry_whole_batch_creates_no_duplicates_or_extra_audit(self):
        entries = [
            {"seq": 1, **observation("E-10", obs_id="obs-10")},
            {"seq": 2, "op": "action", "entity_id": "obs-10", "action": "submit",
             "data": {"location": "North", "observed_at": "2026-04-01"}},
            {"seq": 3, "op": "create", "kind": "sample",
             "data": {"observation_id": "obs-10", "sample_code": "W-10"}, "id": "smp-10"},
        ]
        first = self.offline.submit(self.actor, "B-2", "device-7", entries)
        self.assertEqual(first["confirmed_seq"], 3)
        audits_before = len(self.repo.list_audit())
        entities_before = len(self.repo.list_entities())

        # Device loses the ack and retries the identical batch.
        second = self.offline.submit(self.actor, "B-2", "device-7", entries)
        self.assertEqual(second["confirmed_seq"], 3)
        self.assertEqual([e["status"] for e in second["entries"]], ["applied"] * 3)
        self.assertTrue(all(e["detail"] and e["detail"].get("replay") for e in second["entries"]))
        self.assertEqual(len(self.repo.list_entities()), entities_before)
        self.assertEqual(len(self.repo.list_audit()), audits_before)

    def test_partial_retry_resumes_from_confirmed_point(self):
        # Submit seqs 1-2, then retry with 1-4: only 3 and 4 are applied.
        first = self.offline.submit(
            self.actor, "B-3", "device-7",
            [{"seq": 1, **observation("E-20", obs_id="o20")},
             {"seq": 2, **observation("E-21", observed_at="2026-04-02", obs_id="o21")}],
        )
        self.assertEqual(first["confirmed_seq"], 2)

        audits_before = len(self.repo.list_audit())
        second = self.offline.submit(
            self.actor, "B-3", "device-7",
            [{"seq": 1, **observation("E-20", obs_id="o20")},
             {"seq": 2, **observation("E-21", observed_at="2026-04-02", obs_id="o21")},
             {"seq": 3, **observation("E-22", observed_at="2026-04-03", lat=40.02,
                                      lon=116.02, obs_id="o22")},
             {"seq": 4, "op": "action", "entity_id": "o20", "action": "submit",
              "data": {"location": "North", "observed_at": "2026-04-01"}}],
        )
        self.assertEqual(second["confirmed_seq"], 4)
        self.assertTrue(second["entries"][0]["detail"]["replay"])
        self.assertTrue(second["entries"][1]["detail"]["replay"])
        self.assertNotIn("replay", second["entries"][2].get("detail") or {})
        # Only the two genuinely new entries add audit records.
        self.assertEqual(len(self.repo.list_audit()) - audits_before, 2)

    def test_checkpoint_survives_server_restart(self):
        self.offline.submit(
            self.actor, "B-4", "device-7",
            [{"seq": 1, **observation("E-30", obs_id="o30")}],
        )
        # Restart: brand new repository/service objects, same database file.
        repo, service, offline = self._fresh_services()
        audits_before = len(repo.list_audit())
        result = offline.submit(
            self.actor, "B-4", "device-7",
            [{"seq": 1, **observation("E-30", obs_id="o30")},
             {"seq": 2, **observation("E-31", observed_at="2026-04-02", obs_id="o31")}],
        )
        self.assertEqual(result["confirmed_seq"], 2)
        self.assertTrue(result["entries"][0]["detail"]["replay"])
        self.assertEqual(len(repo.list_audit()) - audits_before, 1)

    # ------------------------------------------------------------------
    # Per-entry rejection isolation
    # ------------------------------------------------------------------
    def test_one_bad_entry_does_not_stop_later_entries(self):
        entries = [
            {"seq": 1, **observation("E-40", obs_id="o40")},
            # Invalid: sample references a missing observation.
            {"seq": 2, "op": "create", "kind": "sample",
             "data": {"observation_id": "missing", "sample_code": "W-40"}, "id": "s40"},
            {"seq": 3, **observation("E-41", observed_at="2026-04-02", obs_id="o41")},
        ]
        result = self.offline.submit(self.actor, "B-5", "device-7", entries)
        self.assertEqual(result["confirmed_seq"], 3)
        self.assertEqual([e["status"] for e in result["entries"]],
                         ["applied", "rejected", "applied"])
        self.assertIn("requires a submitted observation", result["entries"][1]["detail"]["error"])
        self.assertIsNone(self.repo.get_entity("s40"))

    def test_rejected_entry_is_terminal_and_not_retried(self):
        entries = [
            {"seq": 1, **observation("E-50", obs_id="o50")},
            {"seq": 2, "op": "create", "kind": "sample",
             "data": {"observation_id": "o50", "sample_code": "W-50"}},  # obs not submitted yet
        ]
        first = self.offline.submit(self.actor, "B-6", "device-7", entries)
        self.assertEqual(first["entries"][1]["status"], "rejected")

        # Ranger submits the observation first via a later entry, then retries
        # the batch: entry 2 keeps its terminal rejection (stable event
        # numbering never silently mutates semantics).
        second_entries = entries + [
            {"seq": 3, "op": "action", "entity_id": "o50", "action": "submit",
             "data": {"location": "North", "observed_at": "2026-04-01"}},
        ]
        second = self.offline.submit(self.actor, "B-6", "device-7", second_entries)
        self.assertEqual(second["entries"][1]["status"], "rejected")
        self.assertEqual(second["entries"][2]["status"], "applied")

    # ------------------------------------------------------------------
    # Cross-batch duplicates
    # ------------------------------------------------------------------
    def test_duplicate_stable_event_in_another_batch_is_conflict(self):
        self.offline.submit(
            self.actor, "B-7", "device-7",
            [{"seq": 1, **observation("E-60", obs_id="o60", species="deer")}],
        )
        # Same device resends the same stable event id but with changed
        # species in a new batch: conflict, no overwrite, no new sample/audit.
        audits_before = len(self.repo.list_audit())
        result = self.offline.submit(
            self.actor, "B-8", "device-7",
            [{"seq": 1, **observation("E-60", obs_id="o60", species="boar")}],
        )
        self.assertEqual(result["entries"][0]["status"], "rejected")
        self.assertEqual(result["entries"][0]["detail"]["type"], "ConflictError")
        self.assertEqual(self.repo.get_entity("o60")["data"]["species"], "deer")
        self.assertEqual(len(self.repo.list_audit()), audits_before)

    def test_identical_resend_in_another_batch_replays_same_entity(self):
        self.offline.submit(
            self.actor, "B-9", "device-7",
            [{"seq": 1, **observation("E-70", obs_id="o70")}],
        )
        audits_before = len(self.repo.list_audit())
        result = self.offline.submit(
            self.actor, "B-10", "device-7",
            [{"seq": 1, **observation("E-70", obs_id="o70")}],
        )
        self.assertEqual(result["entries"][0]["status"], "applied")
        self.assertEqual(result["entries"][0]["entity_id"], "o70")
        self.assertTrue(result["entries"][0]["detail"]["replay"])
        self.assertEqual(len(self.repo.list_audit()), audits_before)

    # ------------------------------------------------------------------
    # Stale conflicts: lab result / cluster downstream
    # ------------------------------------------------------------------
    def _setup_observation_with_lab_result(self, batch="B-20"):
        entries = [
            {"seq": 1, **observation("E-80", obs_id="o80")},
            {"seq": 2, "op": "action", "entity_id": "o80", "action": "submit",
             "data": {"location": "North", "observed_at": "2026-04-01"}},
            {"seq": 3, "op": "create", "kind": "sample",
             "data": {"observation_id": "o80", "sample_code": "W-80"}, "id": "s80"},
            {"seq": 4, "op": "action", "entity_id": "s80", "action": "send_lab",
             "data": {"lab_id": "LAB-1"}},
            {"seq": 5, "op": "action", "entity_id": "s80", "action": "lab_result",
             "data": {"result": "positive", "result_at": "2026-04-02"}},
        ]
        result = self.offline.submit(self.admin, batch, "device-7", entries)
        self.assertTrue(all(e["status"] == "applied" for e in result["entries"]))
        return self.repo.get_entity("o80"), self.repo.get_entity("s80")

    def test_stale_location_amend_after_lab_result_is_conflict_and_keeps_result(self):
        observation, sample = self._setup_observation_with_lab_result()
        # observation version was bumped by the lab_result conclusion.
        self.assertEqual(sample["status"], "resulted")
        self.assertEqual(sample["data"]["result"], "positive")
        stale_version = 1  # version right after create
        self.assertNotEqual(observation["version"], stale_version)

        result = self.offline.submit(
            self.actor, "B-21", "device-7",
            [{"seq": 1, "op": "amend", "kind": "observation", "entity_id": "o80",
              "data": {"location": "Far Ridge", "lat": 45.0, "lon": 120.0,
                       "observed_at": "2026-04-10"},
              "expected_version": stale_version}],
        )
        entry = result["entries"][0]
        self.assertEqual(entry["status"], "conflict")
        self.assertEqual(entry["detail"]["type"], "StaleConflictError")
        self.assertEqual(entry["detail"]["base_version"], stale_version)
        self.assertEqual(entry["detail"]["server_version"], observation["version"])
        self.assertEqual(entry["detail"]["offline_values"]["location"], "Far Ridge")
        self.assertEqual(entry["detail"]["server_values"]["location"], "North")

        # No overwrite, sample conclusion untouched, no cascade audit.
        obs = self.repo.get_entity("o80")
        self.assertEqual(obs["data"]["location"], "North")
        self.assertEqual(obs["version"], observation["version"])
        smp = self.repo.get_entity("s80")
        self.assertEqual(smp["status"], "resulted")
        self.assertEqual(smp["data"]["result"], "positive")

    def test_current_location_amend_invalidates_result_and_recomputes_cluster(self):
        # Build three submitted observations, samples + positive result on
        # obs-1, then a confirmed cluster over all three.
        entries = []
        for seq, (eid, oid, day) in enumerate([
            ("E-90", "o90", "2026-04-01"),
            ("E-91", "o91", "2026-04-03"),
            ("E-92", "o92", "2026-04-05"),
        ], start=1):
            entries.append({"seq": seq, **observation(eid, observed_at=day,
                                                      lat=40.0 + 0.01 * (seq - 1),
                                                      lon=116.0 + 0.01 * (seq - 1),
                                                      obs_id=oid)})
        entries += [
            {"seq": 4, "op": "action", "entity_id": "o90", "action": "submit",
             "data": {"location": "North", "observed_at": "2026-04-01"}},
            {"seq": 5, "op": "action", "entity_id": "o91", "action": "submit",
             "data": {"location": "North", "observed_at": "2026-04-03"}},
            {"seq": 6, "op": "action", "entity_id": "o92", "action": "submit",
             "data": {"location": "North", "observed_at": "2026-04-05"}},
            {"seq": 7, "op": "create", "kind": "sample",
             "data": {"observation_id": "o90", "sample_code": "W-90"}, "id": "s90"},
            {"seq": 8, "op": "action", "entity_id": "s90", "action": "send_lab",
             "data": {"lab_id": "LAB-1"}},
            {"seq": 9, "op": "action", "entity_id": "s90", "action": "lab_result",
             "data": {"result": "positive", "result_at": "2026-04-02"}},
        ]
        self.offline.submit(self.admin, "B-30", "device-7", entries)

        cluster = self.service.create(self.epi, "cluster", {"region": "North"})
        self.service.transition(
            self.epi, cluster["id"], "confirm_cluster",
            {"observation_ids": ["o90", "o91", "o92"], "centroid": [40.01, 116.01]},
        )
        obs = self.repo.get_entity("o90")
        current_version = obs["version"]

        # Ranger amends the location while based on the *current* version:
        # cascade invalidates the lab result and recomputes the cluster.
        result = self.offline.submit(
            self.actor, "B-31", "device-7",
            [{"seq": 1, "op": "amend", "kind": "observation", "entity_id": "o90",
              "data": {"lat": 40.005, "lon": 116.005},
              "expected_version": current_version}],
        )
        self.assertEqual(result["entries"][0]["status"], "applied")
        detail = result["entries"][0]["detail"]
        self.assertEqual(len(detail["invalidated_samples"]), 1)
        self.assertEqual(detail["invalidated_samples"][0]["sample_id"], "s90")
        self.assertEqual(detail["invalidated_samples"][0]["old_result"]["result"], "positive")
        self.assertEqual(len(detail["recomputed_clusters"]), 1)
        self.assertEqual(detail["recomputed_clusters"][0]["outcome"], "recomputed")

        sample = self.repo.get_entity("s90")
        self.assertEqual(sample["status"], "in_lab")
        self.assertNotIn("result", sample["data"])
        updated_cluster = self.repo.get_entity(cluster["id"])
        self.assertEqual(updated_cluster["status"], "confirmed")
        self.assertEqual(updated_cluster["data"]["centroid"],
                         detail["recomputed_clusters"][0]["centroid"])

        # Old values retained in the audit trail.
        sample_audit = [a for a in self.repo.list_audit("s90")
                        if a["action"] == "invalidate_result"]
        self.assertEqual(sample_audit[0]["detail"]["old_result"]["result"], "positive")
        cluster_audit = [a for a in self.repo.list_audit(cluster["id"])
                         if a["action"] == "recompute_cluster"]
        self.assertEqual(cluster_audit[0]["detail"]["old_centroid"], [40.01, 116.01])

    def test_cluster_is_invalidated_when_amend_breaks_proximity(self):
        entries = []
        for seq, (eid, oid, day) in enumerate([
            ("E-100", "o100", "2026-04-01"),
            ("E-101", "o101", "2026-04-03"),
            ("E-102", "o102", "2026-04-05"),
        ], start=1):
            entries.append({"seq": seq, **observation(eid, observed_at=day,
                                                      lat=40.0 + 0.01 * (seq - 1),
                                                      lon=116.0 + 0.01 * (seq - 1),
                                                      obs_id=oid)})
        entries += [
            {"seq": 4, "op": "action", "entity_id": "o100", "action": "submit",
             "data": {"location": "North", "observed_at": "2026-04-01"}},
            {"seq": 5, "op": "action", "entity_id": "o101", "action": "submit",
             "data": {"location": "North", "observed_at": "2026-04-03"}},
            {"seq": 6, "op": "action", "entity_id": "o102", "action": "submit",
             "data": {"location": "North", "observed_at": "2026-04-05"}},
        ]
        self.offline.submit(self.actor, "B-40", "device-7", entries)
        cluster = self.service.create(self.epi, "cluster", {"region": "North"})
        self.service.transition(
            self.epi, cluster["id"], "confirm_cluster",
            {"observation_ids": ["o100", "o101", "o102"], "centroid": [40.01, 116.01]},
        )
        obs = self.repo.get_entity("o100")
        result = self.offline.submit(
            self.actor, "B-41", "device-7",
            [{"seq": 1, "op": "amend", "kind": "observation", "entity_id": "o100",
              "data": {"lat": 60.0, "lon": 130.0, "observed_at": "2026-04-20"},
              "expected_version": obs["version"]}],
        )
        self.assertEqual(result["entries"][0]["status"], "applied")
        outcome = result["entries"][0]["detail"]["recomputed_clusters"][0]
        self.assertEqual(outcome["outcome"], "invalidated")
        self.assertEqual(self.repo.get_entity(cluster["id"])["status"], "invalidated")

    def test_sample_amend_after_result_invalidates_result_with_audit(self):
        _, sample = self._setup_observation_with_lab_result()
        result = self.offline.submit(
            self.actor, "B-50", "device-7",
            [{"seq": 1, "op": "amend", "kind": "sample", "entity_id": "s80",
              "data": {"storage": "frozen"}, "expected_version": sample["version"]}],
        )
        self.assertEqual(result["entries"][0]["status"], "applied")
        updated = self.repo.get_entity("s80")
        self.assertEqual(updated["status"], "in_lab")
        self.assertEqual(updated["data"]["storage"], "frozen")
        self.assertNotIn("result", updated["data"])
        audit = [a for a in self.repo.list_audit("s80")
                 if a["action"] == "invalidate_result"]
        self.assertEqual(audit[0]["detail"]["old_result"]["result"], "positive")

    def test_non_protected_amend_does_not_touch_downstream(self):
        _, sample = self._setup_observation_with_lab_result()
        obs = self.repo.get_entity("o80")
        result = self.offline.submit(
            self.actor, "B-51", "device-7",
            [{"seq": 1, "op": "amend", "kind": "observation", "entity_id": "o80",
              "data": {"note": "animal limping"}, "expected_version": obs["version"]}],
        )
        self.assertEqual(result["entries"][0]["status"], "applied")
        self.assertEqual(result["entries"][0]["detail"],
                         {"invalidated_samples": [], "recomputed_clusters": []})
        self.assertEqual(self.repo.get_entity("s80")["status"], "resulted")

    def test_stale_amend_after_merge_into_confirmed_cluster_is_conflict(self):
        entries = []
        for seq, (eid, oid, day) in enumerate([
            ("E-200", "o200", "2026-04-01"),
            ("E-201", "o201", "2026-04-03"),
            ("E-202", "o202", "2026-04-05"),
        ], start=1):
            entries.append({"seq": seq, **observation(eid, observed_at=day,
                                                      lat=40.0 + 0.01 * (seq - 1),
                                                      lon=116.0 + 0.01 * (seq - 1),
                                                      obs_id=oid)})
        self.offline.submit(self.actor, "B-100", "device-7", entries)
        created_version = self.repo.get_entity("o200")["version"]
        for oid, day in [("o200", "2026-04-01"), ("o201", "2026-04-03"), ("o202", "2026-04-05")]:
            self.service.transition(
                self.actor, oid, "submit",
                {"location": "North", "observed_at": day},
            )
        cluster = self.service.create(self.epi, "cluster", {"region": "North"})
        self.service.transition(
            self.epi, cluster["id"], "confirm_cluster",
            {"observation_ids": ["o200", "o201", "o202"], "centroid": [40.01, 116.01]},
        )
        # The observation was bumped when the cluster absorbed it.
        self.assertGreater(self.repo.get_entity("o200")["version"], created_version)

        result = self.offline.submit(
            self.actor, "B-101", "device-7",
            [{"seq": 1, "op": "amend", "kind": "observation", "entity_id": "o200",
              "data": {"species": "boar", "observed_at": "2026-04-20",
                       "lat": 60.0, "lon": 130.0},
              "expected_version": created_version}],
        )
        entry = result["entries"][0]
        self.assertEqual(entry["status"], "conflict")
        self.assertEqual(entry["detail"]["type"], "StaleConflictError")
        self.assertEqual(entry["detail"]["offline_values"]["species"], "boar")
        self.assertEqual(entry["detail"]["server_values"]["species"], "deer")
        # Cluster and observation data untouched.
        self.assertEqual(self.repo.get_entity(cluster["id"])["status"], "confirmed")
        self.assertEqual(self.repo.get_entity("o200")["data"]["species"], "deer")

    def test_amend_without_expected_version_is_rejected_envelope(self):
        self.offline.submit(
            self.actor, "B-60", "device-7",
            [{"seq": 1, **observation("E-110", obs_id="o110")}],
        )
        from src.domain import ValidationError
        with self.assertRaises(ValidationError):
            self.offline.submit(
                self.actor, "B-61", "device-7",
                [{"seq": 1, "op": "amend", "kind": "observation",
                  "entity_id": "o110", "data": {"note": "x"}}],
            )

    def test_gap_and_non_positive_seq_rejected_before_anything_is_stored(self):
        from src.domain import ValidationError
        with self.assertRaises(ValidationError):
            self.offline.submit(
                self.actor, "B-70", "device-7",
                [{"seq": 1, **observation("E-120", obs_id="o120")},
                 {"seq": 3, **observation("E-121", obs_id="o121")}],
            )
        self.assertIsNone(self.repo.get_sync_batch("B-70"))
        self.assertIsNone(self.repo.get_entity("o120"))

    def test_resubmitted_seq_with_different_payload_is_conflict(self):
        from src.domain import ConflictError
        self.offline.submit(
            self.actor, "B-80", "device-7",
            [{"seq": 1, "op": "create", "kind": "observation",
              "data": {"event_id": "E-130", "species": "deer",
                       "location": "North", "observed_at": "2026-04-01",
                       "lat": 40.0, "lon": 116.0},
              "id": "o130"}],
        )
        with self.assertRaises(ConflictError):
            self.offline.submit(
                self.actor, "B-80", "device-7",
                [{"seq": 1, "op": "create", "kind": "observation",
                  "data": {"event_id": "E-130", "species": "boar",
                           "location": "North", "observed_at": "2026-04-01",
                           "lat": 40.0, "lon": 116.0},
                  "id": "o130"}],
            )

    def test_status_endpoint_reports_checkpoint(self):
        self.offline.submit(
            self.actor, "B-90", "device-7",
            [{"seq": 1, **observation("E-140", obs_id="o140")}],
        )
        status = self.offline.status("B-90", self.actor)
        self.assertEqual(status["batch_id"], "B-90")
        self.assertEqual(status["confirmed_seq"], 1)
        self.assertEqual(status["next_seq"], 2)
        self.assertEqual(status["entries"][0]["op"], "create")


if __name__ == "__main__":
    unittest.main()

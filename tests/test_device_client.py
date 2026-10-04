import json
import tempfile
import threading
import unittest
import urllib.request
from pathlib import Path

from src.device_client import BatchUploader, DeviceQueue
from src.http_api import create_server
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class _FlakyOpener:
    """Fails the Nth request to simulate losing the link back at the station."""

    def __init__(self, fail_on):
        self.calls = 0
        self.fail_on = set(fail_on)

    def __call__(self, request, *args, **kwargs):
        self.calls += 1
        if self.calls in self.fail_on:
            raise OSError("network unreachable")
        return urllib.request.urlopen(request, *args, **kwargs)


class DeviceClientTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp_dir = tempfile.TemporaryDirectory()
        cls.server_db = str(Path(cls.tmp_dir.name) / "server.db")
        repo = SQLiteRepository(cls.server_db)
        cls.service = DomainService(repo, RuleEngine())
        static_dir = str(Path(__file__).resolve().parent.parent / "static")
        cls.server = create_server("127.0.0.1", 8410, cls.service, RuleEngine(), static_dir)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = "http://127.0.0.1:8410"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.tmp_dir.cleanup()

    def setUp(self):
        self.device_tmp = tempfile.TemporaryDirectory()
        self.device_db = str(Path(self.device_tmp.name) / "device.db")

    def tearDown(self):
        self.device_tmp.cleanup()

    def _observation(self, event_id, day):
        return {"event_id": event_id, "species": "deer", "location": "North",
                "observed_at": day, "lat": 40.0, "lon": 116.0}

    def test_offline_registration_resumable_sync_with_retries_and_restart(self):
        queue = DeviceQueue(self.device_db, "device-42")
        # Ranger works offline for days: stable per-device event numbers.
        seq1, id1 = queue.create("observation", self._observation("D-1", "2026-04-01"))
        seq2, id2 = queue.create("observation", self._observation("D-2", "2026-04-02"))
        seq3, id3 = queue.create("observation", self._observation("D-3", "2026-04-03"))
        self.assertEqual([seq1, seq2, seq3], [1, 2, 3])

        # First sync attempt dies on the PUT (network drops after spool).
        flaky = _FlakyOpener(fail_on={2})  # GET checkpoint (1), PUT fails (2)
        uploader = BatchUploader(queue, self.base, "ranger-1", "field",
                                 "TRIP-1", opener=flaky)
        with self.assertRaises(OSError):
            uploader.sync()

        # Retry with a healthy network resumes from the confirmed point.
        progress = []
        status = BatchUploader(
            queue, self.base, "ranger-1", "field", "TRIP-1",
            opener=urllib.request.urlopen,
        ).sync(progress=lambda confirmed, remaining, body: progress.append((confirmed, remaining)))
        self.assertEqual(status["confirmed_seq"], 3)
        self.assertTrue(progress)
        self.assertEqual(progress[-1], (3, 0))
        self.assertEqual([e["status"] for e in status["entries"]],
                         ["applied", "applied", "applied"])

        # Run sync a third time: nothing new to send, no duplicate entities.
        server_repo = self.service.repository
        before = len(server_repo.list_entities(kind="observation"))
        audits_before = len(server_repo.list_audit())
        status = BatchUploader(
            queue, self.base, "ranger-1", "field", "TRIP-1",
            opener=urllib.request.urlopen,
        ).sync()
        self.assertEqual(status["confirmed_seq"], 3)
        self.assertEqual(len(server_repo.list_entities(kind="observation")), before)
        self.assertEqual(len(server_repo.list_audit()), audits_before)

        # The stable ids assigned offline are the ids the server committed.
        for stable_id in (id1, id2, id3):
            self.assertIsNotNone(server_repo.get_entity(stable_id))

    def test_device_queue_survives_process_restart(self):
        queue = DeviceQueue(self.device_db, "device-77")
        queue.create("observation", self._observation("D-9", "2026-04-01"),
                     stable_id="stable-9")
        # Simulate device process restart: new object, same database file.
        restarted = DeviceQueue(self.device_db, "device-77")
        pending = restarted.pending()
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["stable_id"], "stable-9")
        self.assertEqual(pending[0]["event_seq"], 1)


if __name__ == "__main__":
    unittest.main()

import json
import tempfile
import threading
import unittest
import urllib.request
from pathlib import Path

from src.http_api import create_server
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class HttpSyncTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "http.db")
        self.server, self.base = self._start(self.db_path, 8401)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.tmp.cleanup()

    @staticmethod
    def _start(db_path, port):
        repo = SQLiteRepository(db_path)
        service = DomainService(repo, RuleEngine())
        static_dir = str(Path(__file__).resolve().parent.parent / "static")
        server = create_server("127.0.0.1", port, service, RuleEngine(), static_dir)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        return server, "http://127.0.0.1:%s" % port

    def _request(self, method, path, payload=None, headers=None):
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        for key, value in (headers or {}).items():
            req.add_header(key, value)
        try:
            with urllib.request.urlopen(req) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def _entry(self, seq, event_id, day):
        return {"seq": seq, "op": "create", "kind": "observation",
                "data": {"event_id": event_id, "species": "deer", "location": "North",
                         "observed_at": day, "lat": 40.0, "lon": 116.0}}

    def test_resumable_batch_over_http_with_restart(self):
        head = {"X-User-Id": "ranger-1", "X-Role": "field", "X-Device-Id": "dev-1"}

        # Submit seq 1-2, then the device loses the connection and retries.
        status, body = self._request("PUT", "/api/sync/batches/HTTP-1",
                                     {"device_id": "dev-1",
                                      "entries": [self._entry(1, "E-1", "2026-04-01"),
                                                  self._entry(2, "E-2", "2026-04-02")]},
                                     head)
        self.assertEqual(status, 200)
        self.assertEqual(body["confirmed_seq"], 2)

        # Checkpoint query (the call the device makes after reconnecting).
        status, body = self._request("GET", "/api/sync/HTTP-1", headers=head)
        self.assertEqual(status, 200)
        self.assertEqual(body["next_seq"], 3)

        # Retry 1-2 and extend with 3: the device resumes from confirmed point.
        status, body = self._request("PUT", "/api/sync/batches/HTTP-1",
                                     {"device_id": "dev-1",
                                      "entries": [self._entry(1, "E-1", "2026-04-01"),
                                                  self._entry(2, "E-2", "2026-04-02"),
                                                  self._entry(3, "E-3", "2026-04-03")]},
                                     head)
        self.assertEqual(status, 200)
        self.assertEqual(body["confirmed_seq"], 3)
        self.assertTrue(body["entries"][0]["detail"]["replay"])
        self.assertNotIn("replay", body["entries"][2].get("detail") or {})

        # Restart the station server on a new port, same database, and continue.
        self.server.shutdown()
        self.server.server_close()
        self.server, self.base = self._start(self.db_path, 8402)
        status, body = self._request("PUT", "/api/sync/batches/HTTP-1",
                                     {"device_id": "dev-1",
                                      "entries": [self._entry(3, "E-3", "2026-04-03"),
                                                  self._entry(4, "E-4", "2026-04-04")]},
                                     head)
        self.assertEqual(status, 200)
        self.assertEqual(body["confirmed_seq"], 4)
        # Seqs 1-3 were already confirmed before this call; seq 4 is new.
        self.assertTrue(body["entries"][2]["detail"]["replay"])
        self.assertIsNone(body["entries"][3]["detail"])

        # Gap submission is a 400 and adds nothing.
        status, body = self._request("PUT", "/api/sync/batches/HTTP-2",
                                     {"device_id": "dev-1",
                                      "entries": [self._entry(1, "X-1", "2026-04-01"),
                                                  self._entry(3, "X-3", "2026-04-03")]},
                                     head)
        self.assertEqual(status, 400)
        self.assertEqual(body["type"], "ValidationError")
        status, body = self._request("GET", "/api/sync/HTTP-2", headers=head)
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()

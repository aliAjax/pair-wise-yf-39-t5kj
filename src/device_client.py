"""Device-side resumable uploader for the offline batch protocol.

Rangers register observations and samples with no connectivity.  Every record
is spooled locally in a durable SQLite queue with a stable per-device sequence
number (``event_seq``) and a stable client-generated id; the network is only
used when the device is back at the station.  On sync the device asks the
server for ``confirmed_seq`` and transmits only what is still unacknowledged,
so a dropped connection, a duplicate retry or a device/restart replay can
never create an extra sample or audit record server-side.
"""

import json
import sqlite3
import urllib.error
import urllib.request
from datetime import datetime, timezone
from uuid import uuid4


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class DeviceQueue:
    """Local durable event log on the ranger's device."""

    def __init__(self, path, device_id):
        self.path = str(path)
        self.device_id = device_id
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self):
        with self._connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS outbox (
                    event_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    request TEXT NOT NULL,
                    stable_id TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sync_state (
                    batch_id TEXT PRIMARY KEY,
                    confirmed_seq INTEGER NOT NULL DEFAULT 0
                );
            """)

    def local_confirmed(self, batch_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT confirmed_seq FROM sync_state WHERE batch_id = ?", (batch_id,)
            ).fetchone()
        return int(row["confirmed_seq"]) if row else 0

    def mark_confirmed(self, batch_id, seq):
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO sync_state(batch_id, confirmed_seq) VALUES (?, ?) "
                "ON CONFLICT(batch_id) DO UPDATE SET confirmed_seq = excluded.confirmed_seq",
                (batch_id, seq),
            )

    def register(self, op, request, stable_id=None):
        """Add one observation/sample/action to the outbox while offline."""
        stable_id = stable_id or str(uuid4())
        with self._connect() as connection:
            cur = connection.execute(
                "INSERT INTO outbox(request, stable_id, created_at) VALUES (?, ?, ?)",
                (json.dumps(dict(request, op=op), ensure_ascii=False, sort_keys=True),
                 stable_id, _now()),
            )
            event_seq = cur.lastrowid
        return event_seq, stable_id

    def create(self, kind, data, stable_id=None):
        return self.register("create", {"kind": kind, "data": data}, stable_id)

    def amend(self, kind, entity_id, data, expected_version, stable_id=None):
        return self.register("amend", {
            "kind": kind, "entity_id": entity_id, "data": data,
            "expected_version": expected_version,
        }, stable_id)

    def action(self, entity_id, action, data=None, expected_version=None, stable_id=None):
        request = {"entity_id": entity_id, "action": action, "data": data or {}}
        if expected_version is not None:
            request["expected_version"] = expected_version
        return self.register("action", request, stable_id)

    def pending(self, after_seq=0):
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM outbox WHERE event_seq > ? ORDER BY event_seq",
                (after_seq,),
            ).fetchall()
        return [
            {"event_seq": int(row["event_seq"]),
             "stable_id": row["stable_id"],
             "request": json.loads(row["request"])}
            for row in rows
        ]

    def count(self, after_seq=0):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS n FROM outbox WHERE event_seq > ?", (after_seq,)
            ).fetchone()
        return int(row["n"])


class BatchUploader:
    """Uploads a DeviceQueue to the station with checkpoint continuation.

    One station batch id covers the whole trip.  Each PUT carries a
    gap-free run starting from the server's last confirmed point; retries
    resend the identical numbered entries, which the server replays from its
    durable store rather than applying twice.
    """

    def __init__(self, queue, base_url, user_id, role, batch_id,
                 page_size=100, opener=None):
        self.queue = queue
        self.base_url = base_url.rstrip("/")
        self.user_id = user_id
        self.role = role
        self.batch_id = batch_id
        self.page_size = page_size
        self._opener = opener or urllib.request.urlopen

    def _http(self, method, path, payload=None):
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8") if payload else None
        request = urllib.request.Request(self.base_url + path, data=data, method=method)
        request.add_header("Content-Type", "application/json; charset=utf-8")
        request.add_header("X-User-Id", self.user_id)
        request.add_header("X-Role", self.role)
        request.add_header("X-Device-Id", self.queue.device_id)
        with self._opener(request) as response:
            return json.loads(response.read().decode("utf-8"))

    def checkpoint(self):
        """Server's confirmed point; 0 when the batch is unknown (new trip)."""
        try:
            body = self._http("GET", "/api/sync/" + self.batch_id)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return 0
            raise
        return int(body["confirmed_seq"])

    def sync(self, progress=None):
        """Push every unacknowledged event, continuing from confirmed_seq.

        The page always starts at the server's confirmed point + 1, even if
        the device restarted, so previously acknowledged events are merely
        replayed and never duplicated.  ``progress(confirmed_seq, remaining,
        body)`` is called after each successful page.  Returns the final
        status, whose ``entries`` carry per-entry ``applied``/``conflict``/
        ``rejected`` outcomes for the ranger to review.
        """
        status = None
        while True:
            confirmed = self.checkpoint()
            self.queue.mark_confirmed(self.batch_id, confirmed)
            pending = self.queue.pending(after_seq=confirmed)
            if not pending:
                return status or self._http("GET", "/api/sync/" + self.batch_id)
            page = pending[: self.page_size]
            entries = []
            for item in page:
                entry = dict(item["request"], seq=item["event_seq"])
                if entry["op"] == "create":
                    entry["id"] = item["stable_id"]
                entries.append(entry)
            status = self._http("PUT", "/api/sync/batches/" + self.batch_id, {
                "device_id": self.queue.device_id,
                "entries": entries,
            })
            confirmed = int(status["confirmed_seq"])
            self.queue.mark_confirmed(self.batch_id, confirmed)
            remaining = self.queue.count(after_seq=confirmed)
            if progress:
                progress(confirmed, remaining, status)
            if remaining == 0:
                return status

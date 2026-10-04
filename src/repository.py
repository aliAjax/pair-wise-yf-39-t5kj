import json
import sqlite3
from datetime import datetime, timezone

from .domain import ConflictError, NotFoundError


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class SQLiteRepository:
    def __init__(self, path):
        self.path = str(path)
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self):
        with self._connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS entities (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_entities_kind_status
                    ON entities(kind, status);
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    actor_role TEXT NOT NULL,
                    action TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_audit_entity
                    ON audit_log(entity_id, id);
                CREATE TABLE IF NOT EXISTS idempotency (
                    actor_id TEXT NOT NULL,
                    idem_key TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(actor_id, idem_key)
                );
                CREATE TABLE IF NOT EXISTS offline_batches (
                    batch_id TEXT PRIMARY KEY,
                    device_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS offline_batch_items (
                    batch_id TEXT NOT NULL,
                    item_id TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    op TEXT NOT NULL,
                    kind TEXT,
                    status TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    result TEXT,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(batch_id, item_id)
                );
                CREATE INDEX IF NOT EXISTS idx_offline_items_seq
                    ON offline_batch_items(batch_id, seq);
                CREATE TABLE IF NOT EXISTS recompute_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    ref_entity_id TEXT,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    done_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_recompute_pending
                    ON recompute_queue(status, id);
            """)

    @staticmethod
    def _entity_from_row(row):
        return {
            "id": row["id"],
            "kind": row["kind"],
            "status": row["status"],
            "version": int(row["version"]),
            "data": json.loads(row["data"]),
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def create_entity(self, entity_id, kind, status, data, actor_id):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
                (entity_id, kind, status, payload, actor_id, now, now),
            )
        return self.get_entity(entity_id)

    def get_entity(self, entity_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
        return self._entity_from_row(row) if row else None

    def list_entities(self, kind=None, status=None):
        clauses = []
        params = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM entities" + where + " ORDER BY created_at, id", params
            ).fetchall()
        return [self._entity_from_row(row) for row in rows]

    def find_entities(self, kind, field, value):
        return [
            entity
            for entity in self.list_entities(kind=kind)
            if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
        ]

    def update_entity(self, entity_id, expected_version, status, data):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT version FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("entity not found: " + entity_id)
            current_version = int(row["version"])
            if expected_version is not None and current_version != int(expected_version):
                raise ConflictError(
                    "version conflict: expected %s, found %s"
                    % (expected_version, current_version)
                )
            connection.execute(
                "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
                "WHERE id = ? AND version = ?",
                (status, payload, now, entity_id, current_version),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(entity_id)

    def append_audit(self, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, to_status, detail, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    entity_id,
                    actor_id,
                    actor_role,
                    action,
                    from_status,
                    to_status,
                    json.dumps(detail, ensure_ascii=False, sort_keys=True),
                    utcnow(),
                ),
            )

    def list_audit(self, entity_id=None):
        with self._connect() as connection:
            if entity_id:
                rows = connection.execute(
                    "SELECT * FROM audit_log WHERE entity_id = ? ORDER BY id", (entity_id,)
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM audit_log ORDER BY id").fetchall()
        return [
            {
                "id": row["id"],
                "entity_id": row["entity_id"],
                "actor_id": row["actor_id"],
                "actor_role": row["actor_role"],
                "action": row["action"],
                "from_status": row["from_status"],
                "to_status": row["to_status"],
                "detail": json.loads(row["detail"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def get_idempotency(self, actor_id, idem_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT entity_id FROM idempotency WHERE actor_id = ? AND idem_key = ?",
                (actor_id, idem_key),
            ).fetchone()
        return row["entity_id"] if row else None

    def save_idempotency(self, actor_id, idem_key, entity_id):
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO idempotency(actor_id, idem_key, entity_id, created_at) "
                "VALUES (?, ?, ?, ?)",
                (actor_id, idem_key, entity_id, utcnow()),
            )

    # ------------------------------------------------------------------
    # Offline batches
    # ------------------------------------------------------------------

    def create_offline_batch(self, batch_id, device_id, actor_id):
        now = utcnow()
        with self._connect() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO offline_batches(batch_id, device_id, actor_id, status, created_at, updated_at) "
                "VALUES (?, ?, ?, 'pending', ?, ?)",
                (batch_id, device_id, actor_id, now, now),
            )
        return self.get_offline_batch(batch_id)

    def get_offline_batch(self, batch_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM offline_batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
        if not row:
            return None
        return {
            "batch_id": row["batch_id"],
            "device_id": row["device_id"],
            "actor_id": row["actor_id"],
            "status": row["status"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def update_offline_batch_status(self, batch_id, status):
        with self._connect() as connection:
            connection.execute(
                "UPDATE offline_batches SET status = ?, updated_at = ? WHERE batch_id = ?",
                (status, utcnow(), batch_id),
            )
        return self.get_offline_batch(batch_id)

    def upsert_offline_batch_item(self, batch_id, item_id, seq, op, kind, payload):
        now = utcnow()
        with self._connect() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO offline_batch_items"
                "(batch_id, item_id, seq, op, kind, status, payload, result, error, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, 'pending', ?, NULL, NULL, ?, ?)",
                (batch_id, item_id, seq, op, kind, json.dumps(payload, ensure_ascii=False, sort_keys=True), now, now),
            )
        return self.get_offline_batch_item(batch_id, item_id)

    def get_offline_batch_item(self, batch_id, item_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM offline_batch_items WHERE batch_id = ? AND item_id = ?",
                (batch_id, item_id),
            ).fetchone()
        return self._batch_item_from_row(row) if row else None

    def list_offline_batch_items(self, batch_id):
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM offline_batch_items WHERE batch_id = ? ORDER BY seq, rowid",
                (batch_id,),
            ).fetchall()
        return [self._batch_item_from_row(row) for row in rows]

    @staticmethod
    def _batch_item_from_row(row):
        return {
            "batch_id": row["batch_id"],
            "item_id": row["item_id"],
            "seq": int(row["seq"]),
            "op": row["op"],
            "kind": row["kind"],
            "status": row["status"],
            "payload": json.loads(row["payload"]),
            "result": json.loads(row["result"]) if row["result"] is not None else None,
            "error": json.loads(row["error"]) if row["error"] is not None else None,
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def update_offline_batch_item(self, batch_id, item_id, status, result=None, error=None):
        now = utcnow()
        with self._connect() as connection:
            connection.execute(
                "UPDATE offline_batch_items SET status = ?, result = ?, error = ?, updated_at = ? "
                "WHERE batch_id = ? AND item_id = ?",
                (
                    status,
                    json.dumps(result, ensure_ascii=False, sort_keys=True) if result is not None else None,
                    json.dumps(error, ensure_ascii=False, sort_keys=True) if error is not None else None,
                    now,
                    batch_id,
                    item_id,
                ),
            )
        return self.get_offline_batch_item(batch_id, item_id)

    # ------------------------------------------------------------------
    # Recompute queue
    # ------------------------------------------------------------------

    def enqueue_recompute(self, entity_id, kind, reason, ref_entity_id=None):
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO recompute_queue(entity_id, kind, reason, ref_entity_id, status, created_at, done_at) "
                "VALUES (?, ?, ?, ?, 'pending', ?, NULL)",
                (entity_id, kind, reason, ref_entity_id, utcnow()),
            )

    def list_pending_recompute(self):
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM recompute_queue WHERE status = 'pending' ORDER BY id"
            ).fetchall()
        return [
            {
                "id": row["id"],
                "entity_id": row["entity_id"],
                "kind": row["kind"],
                "reason": row["reason"],
                "ref_entity_id": row["ref_entity_id"],
                "status": row["status"],
                "created_at": row["created_at"],
                "done_at": row["done_at"],
            }
            for row in rows
        ]

    def mark_recompute_done(self, queue_id):
        with self._connect() as connection:
            connection.execute(
                "UPDATE recompute_queue SET status = 'done', done_at = ? WHERE id = ?",
                (utcnow(), queue_id),
            )

    def count_audit(self, entity_id=None):
        with self._connect() as connection:
            if entity_id:
                row = connection.execute(
                    "SELECT COUNT(*) AS c FROM audit_log WHERE entity_id = ?", (entity_id,)
                ).fetchone()
            else:
                row = connection.execute("SELECT COUNT(*) AS c FROM audit_log").fetchone()
        return int(row["c"])

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True

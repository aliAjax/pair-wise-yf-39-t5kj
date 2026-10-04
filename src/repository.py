import json
import sqlite3
from datetime import datetime, timezone

from .domain import ConflictError, NotFoundError, ValidationError


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
                CREATE TABLE IF NOT EXISTS sync_batches (
                    batch_id TEXT PRIMARY KEY,
                    device_id TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_role TEXT NOT NULL,
                    confirmed_seq INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sync_entries (
                    batch_id TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    request TEXT NOT NULL,
                    status TEXT NOT NULL,
                    entity_id TEXT,
                    result TEXT,
                    detail TEXT,
                    processed_at TEXT,
                    PRIMARY KEY(batch_id, seq)
                );
                CREATE INDEX IF NOT EXISTS idx_sync_entries_batch
                    ON sync_entries(batch_id, seq);
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

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True

    # ------------------------------------------------------------------
    # Offline sync batches
    # ------------------------------------------------------------------
    def get_sync_batch(self, batch_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM sync_batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
        return dict(row) if row else None

    def ensure_sync_batch(self, batch_id, device_id, actor):
        """Create the batch row on first sight; later calls are idempotent."""
        now = utcnow()
        with self._connect() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO sync_batches(batch_id, device_id, created_by, created_role, "
                "confirmed_seq, created_at, updated_at) VALUES (?, ?, ?, ?, 0, ?, ?)",
                (batch_id, device_id, actor.user_id, actor.role, now, now),
            )
        return self.get_sync_batch(batch_id)

    def get_sync_entry(self, batch_id, seq):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM sync_entries WHERE batch_id = ? AND seq = ?",
                (batch_id, seq),
            ).fetchone()
        return self._entry_from_row(row) if row else None

    def list_sync_entries(self, batch_id):
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM sync_entries WHERE batch_id = ? ORDER BY seq",
                (batch_id,),
            ).fetchall()
        return [self._entry_from_row(row) for row in rows]

    @staticmethod
    def _entry_from_row(row):
        return {
            "batch_id": row["batch_id"],
            "seq": int(row["seq"]),
            "request": json.loads(row["request"]),
            "status": row["status"],
            "entity_id": row["entity_id"],
            "result": json.loads(row["result"]) if row["result"] else None,
            "detail": json.loads(row["detail"]) if row["detail"] else None,
            "processed_at": row["processed_at"],
        }

    def spool_sync_entries(self, batch_id, entries):
        """Durably store pending entries. Entries already processed are
        untouched; conflicting payloads at an existing seq are rejected."""
        now = utcnow()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            for seq, request in entries:
                existing = connection.execute(
                    "SELECT request, status FROM sync_entries WHERE batch_id = ? AND seq = ?",
                    (batch_id, seq),
                ).fetchone()
                if existing:
                    stored_request = json.loads(existing["request"])
                    if stored_request != request:
                        raise ConflictError(
                            "entry %s was already submitted with different data" % seq
                        )
                    continue
                connection.execute(
                    "INSERT INTO sync_entries(batch_id, seq, request, status) "
                    "VALUES (?, ?, ?, 'pending')",
                    (batch_id, seq, json.dumps(request, ensure_ascii=False, sort_keys=True)),
                )
            connection.execute(
                "UPDATE sync_batches SET updated_at = ? WHERE batch_id = ?",
                (now, batch_id),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def process_pending_entries(self, batch_id, apply_entry):
        """Apply every pending entry in order, inside one durable transaction.

        Each entry is isolated by a savepoint: a domain failure marks only
        that entry as conflict/rejected (nothing is written) and processing
        continues. The batch checkpoint advances only to entries that have a
        terminal outcome, so a crash anywhere replays nothing and skips
        nothing.
        """
        connection = self._connect()
        tx = None
        try:
            connection.execute("BEGIN IMMEDIATE")
            batch = connection.execute(
                "SELECT * FROM sync_batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if not batch:
                raise NotFoundError("batch not found: " + batch_id)
            tx = _Tx(connection)
            confirmed = int(batch["confirmed_seq"])
            rows = connection.execute(
                "SELECT * FROM sync_entries WHERE batch_id = ? ORDER BY seq",
                (batch_id,),
            ).fetchall()
            if rows and int(rows[0]["seq"]) != 1:
                raise ValidationError("batch entries must start at seq 1")
            outcomes = []
            gap = False
            for index, row in enumerate(rows, start=1):
                seq = int(row["seq"])
                if seq != index:
                    raise ValidationError(
                        "gap in batch entries: expected seq %s, found %s" % (index, seq)
                    )
                if row["status"] != "pending":
                    outcomes.append(self._entry_from_row(row))
                    continue
                request = json.loads(row["request"])
                savepoint = "sp_%s" % seq
                connection.execute("SAVEPOINT %s" % savepoint)
                try:
                    outcome = apply_entry(tx, seq, request)
                    outcome = dict(outcome)
                    outcome.setdefault("seq", seq)
                    status = outcome.get("status", "applied")
                    entity_id = outcome.get("entity_id")
                    result = outcome.get("result")
                    detail = outcome.get("detail")
                    connection.execute(
                        "UPDATE sync_entries SET status = ?, entity_id = ?, result = ?, "
                        "detail = ?, processed_at = ? WHERE batch_id = ? AND seq = ?",
                        (
                            status,
                            entity_id,
                            json.dumps(result, ensure_ascii=False, sort_keys=True) if result is not None else None,
                            json.dumps(detail, ensure_ascii=False, sort_keys=True) if detail is not None else None,
                            utcnow(),
                            batch_id,
                            seq,
                        ),
                    )
                    connection.execute("RELEASE SAVEPOINT %s" % savepoint)
                    confirmed = seq
                except _EntryRejected as rejected:
                    connection.execute("ROLLBACK TO SAVEPOINT %s" % savepoint)
                    connection.execute("RELEASE SAVEPOINT %s" % savepoint)
                    connection.execute(
                        "UPDATE sync_entries SET status = ?, detail = ?, processed_at = ? "
                        "WHERE batch_id = ? AND seq = ?",
                        (
                            rejected.status,
                            json.dumps(rejected.details, ensure_ascii=False, sort_keys=True),
                            utcnow(),
                            batch_id,
                            seq,
                        ),
                    )
                    outcome = {
                        "seq": seq,
                        "status": rejected.status,
                        "entity_id": None,
                        "result": None,
                        "detail": rejected.details,
                    }
                    confirmed = seq
                outcomes.append(outcome)
            connection.execute(
                "UPDATE sync_batches SET confirmed_seq = ?, updated_at = ? WHERE batch_id = ?",
                (confirmed, utcnow(), batch_id),
            )
            connection.commit()
            return confirmed, outcomes
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()


class _EntryRejected(Exception):
    """Internal signal: this entry must be recorded as non-applied."""

    def __init__(self, status, details):
        super().__init__(details.get("error", status))
        self.status = status
        self.details = details


class _Tx:
    """Repository primitives bound to one connection/transaction.

    Offline entry application (entity writes, audit, and downstream cascade)
    all go through this object so the whole entry commits or rolls back
    together with the checkpoint advance.
    """

    def __init__(self, connection):
        self.connection = connection

    def _entity_from_row(self, row):
        return SQLiteRepository._entity_from_row(row)

    def get_entity(self, entity_id):
        row = self.connection.execute(
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
        rows = self.connection.execute(
            "SELECT * FROM entities" + where + " ORDER BY created_at, id", params
        ).fetchall()
        return [self._entity_from_row(row) for row in rows]

    def find_entities(self, kind, field, value):
        return [
            entity
            for entity in self.list_entities(kind=kind)
            if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
        ]

    def create_entity(self, entity_id, kind, status, data, actor_id):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        self.connection.execute(
            "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
            "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
            (entity_id, kind, status, payload, actor_id, now, now),
        )
        return self.get_entity(entity_id)

    def update_entity(self, entity_id, expected_version, status, data):
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        row = self.connection.execute(
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
        self.connection.execute(
            "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
            "WHERE id = ? AND version = ?",
            (status, payload, utcnow(), entity_id, current_version),
        )
        return self.get_entity(entity_id)

    def append_audit(self, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
        self.connection.execute(
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

    def record_audit(self, entity_id, actor, action, from_status, to_status, detail=None):
        self.append_audit(
            entity_id=entity_id,
            actor_id=actor.user_id,
            actor_role=actor.role,
            action=action,
            from_status=from_status,
            to_status=to_status,
            detail=detail or {},
        )

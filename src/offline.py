"""Resumable offline batches.

Rangers register observations and samples while the device is offline.  Back at
the station the device submits a batch where every entry has a stable
``seq`` (and entities may carry a stable client id).  The batch, its entries,
their outcomes and the checkpoint (``confirmed_seq``) are all durable, so:

* retries and restarts resume strictly after the last confirmed entry;
* a replayed entry returns the stored outcome and creates no extra entity or
  audit record;
* an offline amend based on a stale version is returned as a *conflict* when
  the server has already derived lab results or confirmed cluster conclusions
  downstream -- the stale location/species/time never overwrites them;
* an amend based on the current version is applied, and the derived sample
  results / cluster conclusions are invalidated and recomputed, with every old
  value retained in the audit trail.
"""

from .domain import (
    ConflictError,
    InvalidTransition,
    NotFoundError,
    PermissionDenied,
    StaleConflictError,
    ValidationError,
)
from .repository import _EntryRejected
from .rules import (
    OBSERVATION_PROTECTED_FIELDS,
    OFFLINE_AMEND_FIELDS,
    RuleEngine,
)
from .rules import _date_ordinal, _haversine_km
from uuid import uuid4

_NATURAL_KEY = {
    "observation": ("event_id", "observed_at"),
    "sample": ("sample_code",),
}
# Fields that belong to the lab conclusion and are stripped when a derived
# result is invalidated.  The values themselves survive in the audit log.
_LAB_RESULT_FIELDS = ("result", "result_at")


class OfflineBatchService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()

    def _lookup(self, store, kind, field, value):
        return store.find_entities(self.rules.normalize_kind(kind), field, value)

    # ------------------------------------------------------------------
    # API
    # ------------------------------------------------------------------
    def submit(self, actor, batch_id, device_id, raw_entries):
        if not batch_id:
            raise ValidationError("batch_id is required")
        if not device_id:
            raise ValidationError("device_id is required")
        if not isinstance(raw_entries, list) or not raw_entries:
            raise ValidationError("entries must be a non-empty list")

        normalized = []
        seen = set()
        for raw in raw_entries:
            if not isinstance(raw, dict):
                raise ValidationError("each entry must be an object")
            seq = raw.get("seq")
            if not isinstance(seq, int) or isinstance(seq, bool) or seq < 1:
                raise ValidationError("entry seq must be a positive integer")
            if seq in seen:
                raise ValidationError("duplicate seq in batch: %s" % seq)
            seen.add(seq)
            op = raw.get("op")
            if op not in ("create", "amend", "action"):
                raise ValidationError("entry %s: op must be create/amend/action" % seq)
            request = self._normalize_request(seq, op, raw)
            normalized.append((seq, request))

        # The numbered log must be gap-free.  Entries already stored are
        # allowed (a retry); new seqs must continue contiguously after them.
        normalized.sort(key=lambda item: item[0])
        existing_batch = self.repository.get_sync_batch(batch_id)
        known_seqs = {
            entry["seq"] for entry in self.repository.list_sync_entries(batch_id)
        } if existing_batch else set()
        known_seqs.update(seq for seq, _ in normalized)
        if known_seqs:
            for required in range(1, max(known_seqs) + 1):
                if required not in known_seqs:
                    raise ValidationError(
                        "gap in batch entries: missing seq %s" % required
                    )

        # Durable spool first: once this returns, a network drop or restart
        # loses nothing even if processing never started.
        pre_stored_seqs = (
            {entry["seq"] for entry in self.repository.list_sync_entries(batch_id)}
            if existing_batch else set()
        )
        self.repository.ensure_sync_batch(batch_id, device_id, actor)
        self.repository.spool_sync_entries(batch_id, normalized)
        confirmed, outcomes = self.repository.process_pending_entries(
            batch_id, lambda tx, seq, request: self._apply_entry(tx, actor, seq, request)
        )
        result = self.status(batch_id, actor, confirmed=confirmed, outcomes=outcomes)
        # Entries already acknowledged in a previous call are replays: their
        # stored outcome is returned and nothing was created again.
        for entry in result["entries"]:
            if (entry["seq"] in pre_stored_seqs and entry["status"] == "applied"
                    and not entry["detail"]):
                entry["detail"] = {"replay": True}
        return result

    @staticmethod
    def _normalize_request(seq, op, raw):
        if op == "create":
            kind = raw.get("kind")
            data = raw.get("data")
            if not kind:
                raise ValidationError("entry %s: kind is required" % seq)
            if not isinstance(data, dict):
                raise ValidationError("entry %s: data must be an object" % seq)
            request = {"op": op, "kind": kind, "data": dict(data)}
            if raw.get("id"):
                request["id"] = raw["id"]
            return request
        if op == "amend":
            kind = raw.get("kind")
            entity_id = raw.get("entity_id") or raw.get("id")
            data = raw.get("data")
            if not kind:
                raise ValidationError("entry %s: kind is required" % seq)
            if not entity_id:
                raise ValidationError("entry %s: entity_id is required" % seq)
            if not isinstance(data, dict) or not data:
                raise ValidationError("entry %s: data must be a non-empty object" % seq)
            if raw.get("expected_version") is None:
                raise ValidationError(
                    "entry %s: expected_version is required for amend" % seq
                )
            return {
                "op": op,
                "kind": kind,
                "entity_id": entity_id,
                "data": dict(data),
                "expected_version": int(raw["expected_version"]),
            }
        # action
        entity_id = raw.get("entity_id") or raw.get("id")
        action = raw.get("action")
        if not entity_id:
            raise ValidationError("entry %s: entity_id is required" % seq)
        if not action:
            raise ValidationError("entry %s: action is required" % seq)
        return {
            "op": op,
            "entity_id": entity_id,
            "action": action,
            "data": dict(raw.get("data") or {}),
            "expected_version": raw.get("expected_version"),
        }

    def status(self, batch_id, actor, confirmed=None, outcomes=None):
        batch = self.repository.get_sync_batch(batch_id)
        if not batch:
            raise NotFoundError("batch not found: " + batch_id)
        if outcomes is None:
            stored = self.repository.list_sync_entries(batch_id)
            confirmed = batch["confirmed_seq"]
        else:
            by_seq = {item["seq"]: item for item in outcomes}
            stored = self.repository.list_sync_entries(batch_id)
            for entry in stored:
                fresh = by_seq.get(entry["seq"])
                if fresh and entry["status"] == "pending":
                    entry.update(fresh)
        entries = [
            {
                "seq": entry["seq"],
                "op": entry["request"].get("op"),
                "status": entry["status"],
                "entity_id": entry["entity_id"],
                "result": entry["result"],
                "detail": entry["detail"],
            }
            for entry in stored
        ]
        return {
            "batch_id": batch_id,
            "device_id": batch["device_id"],
            "confirmed_seq": confirmed if confirmed is not None else batch["confirmed_seq"],
            "next_seq": (confirmed if confirmed is not None else batch["confirmed_seq"]) + 1,
            "entries": entries,
        }

    # ------------------------------------------------------------------
    # Entry application (runs inside the batch transaction)
    # ------------------------------------------------------------------
    def _apply_entry(self, tx, actor, seq, request):
        try:
            op = request["op"]
            if op == "create":
                return self._apply_create(tx, actor, request)
            if op == "amend":
                return self._apply_amend(tx, actor, request)
            return self._apply_action(tx, actor, request)
        except StaleConflictError as exc:
            # Stale location/species/time is reported, never written.
            raise _EntryRejected("conflict", {
                "error": str(exc), "type": type(exc).__name__, **exc.details,
            })
        except (ConflictError, InvalidTransition, NotFoundError,
                PermissionDenied, ValidationError) as exc:
            raise _EntryRejected("rejected", {
                "error": str(exc), "type": type(exc).__name__,
            })

    def _apply_create(self, tx, actor, request):
        kind = self.rules.normalize_kind(request["kind"])
        if kind not in ("observation", "sample"):
            raise ValidationError("offline batches only accept observation/sample creates")
        payload = dict(request["data"])

        # Natural-key replay/conflict against entities from any batch (this is
        # what makes a whole-batch retry idempotent too).
        matched = self._find_natural(tx, kind, payload)
        if matched:
            if self._same_payload(kind, matched["data"], payload):
                return {"status": "applied", "entity_id": matched["id"],
                        "result": matched, "detail": {"replay": True}}
            raise ConflictError(
                "%s %s already exists with different data"
                % (kind, matched["id"])
            )

        self.rules.validate_create(actor, kind, payload, lambda k, f, v: self._lookup(tx, k, f, v))
        entity_id = str(payload.pop("id", "") or request.get("id") or uuid4())
        if tx.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind)
        entity = tx.create_entity(entity_id, kind, status, payload, actor.user_id)
        tx.record_audit(entity_id, actor, "create", None, status, {"kind": kind, "offline": True})
        return {"status": "applied", "entity_id": entity_id, "result": entity}

    def _find_natural(self, tx, kind, payload):
        fields = _NATURAL_KEY.get(kind)
        if not fields:
            return None
        candidates = None
        for field in fields:
            value = payload.get(field)
            if value is None:
                return None
            rows = self._lookup(tx, kind, field, value)
            candidates = rows if candidates is None else [r for r in candidates if r in rows]
        for entity in candidates or []:
            if all(entity["data"].get(field) == payload.get(field) for field in fields):
                return entity
        return None

    @staticmethod
    def _same_payload(kind, stored, incoming):
        if kind == "observation":
            keys = RuleEngine.CREATE_REQUIRED["observation"]
        else:
            keys = ("sample_code",)
        return all(stored.get(key) == incoming.get(key) for key in keys)

    def _apply_amend(self, tx, actor, request):
        kind = self.rules.normalize_kind(request["kind"])
        if kind not in ("observation", "sample"):
            raise ValidationError("offline amends only apply to observation/sample")
        entity = tx.get_entity(request["entity_id"])
        if not entity:
            raise NotFoundError("entity not found: " + request["entity_id"])
        if entity["kind"] != kind:
            raise ValidationError(
                "entity %s is a %s, not a %s" % (entity["id"], entity["kind"], kind)
            )

        expected = request["expected_version"]
        if entity["version"] != expected:
            # The server moved on (e.g. lab result arrived): report conflict,
            # return both sides, write nothing.
            raise StaleConflictError(
                "stale amend on %s %s: base version %s, server version %s"
                % (kind, entity["id"], expected, entity["version"]),
                {
                    "entity_id": entity["id"],
                    "kind": kind,
                    "base_version": expected,
                    "server_version": entity["version"],
                    "offline_values": request["data"],
                    "server_values": {
                        key: entity["data"].get(key) for key in request["data"]
                    },
                },
            )

        patch = dict(request["data"])
        allowed = OFFLINE_AMEND_FIELDS[kind]
        unknown = [key for key in patch if key not in allowed]
        if unknown:
            raise ValidationError("field cannot be amended offline: " + ", ".join(sorted(unknown)))

        if kind == "observation":
            return self._amend_observation(tx, actor, entity, expected, patch)
        return self._amend_sample(tx, actor, entity, expected, patch)

    def _amend_observation(self, tx, actor, observation, expected, patch):
        merged = dict(observation["data"])
        merged.update(patch)
        updated = tx.update_entity(
            observation["id"], expected, observation["status"], merged
        )
        tx.record_audit(
            observation["id"], actor, "amend",
            observation["status"], updated["status"],
            {"patch": patch, "old_values": {k: observation["data"].get(k) for k in patch}},
        )
        detail = {"invalidated_samples": [], "recomputed_clusters": []}
        protected = any(key in OBSERVATION_PROTECTED_FIELDS for key in patch)
        if protected:
            # Derived lab conclusions are invalidated (old values audited on
            # the sample), and every confirmed cluster containing this
            # observation is recomputed or invalidated.
            for sample in self._linked_samples(tx, observation["id"]):
                if sample["status"] in ("resulted", "closed"):
                    detail["invalidated_samples"].append(
                        self._invalidate_sample_result(tx, actor, sample, "observation_amend")
                    )
            for cluster in self._confirmed_clusters(tx, observation["id"]):
                detail["recomputed_clusters"].append(
                    self._recompute_cluster(tx, actor, cluster, updated)
                )
        return {"status": "applied", "entity_id": updated["id"],
                "result": updated, "detail": detail}

    def _amend_sample(self, tx, actor, sample, expected, patch):
        detail = {}
        status = sample["status"]
        data = dict(sample["data"])
        version = expected
        if sample["status"] in ("resulted", "closed"):
            # The edit invalidates the lab conclusion; the sample goes back to
            # the lab and the old result is kept in the audit log only.
            invalidated = self._invalidate_sample_result(tx, actor, sample, "sample_amend")
            status = invalidated["status"]
            data = invalidated["data"]
            version = None  # invalidate_result already advanced the version
            detail["lab_result_invalidated"] = True
        data.update(patch)
        updated = tx.update_entity(sample["id"], version, status, data)
        tx.record_audit(
            sample["id"], actor, "amend",
            sample["status"], updated["status"],
            {"patch": patch, "old_values": {k: sample["data"].get(k) for k in patch}},
        )
        return {"status": "applied", "entity_id": updated["id"],
                "result": updated, "detail": detail}

    def _apply_action(self, tx, actor, request):
        entity = tx.get_entity(request["entity_id"])
        if not entity:
            raise NotFoundError("entity not found: " + request["entity_id"])
        expected = request.get("expected_version")
        next_status, patch = self.rules.validate_transition(
            actor, entity, request["action"], dict(request["data"]),
            lambda k, f, v: self._lookup(tx, k, f, v),
        )
        merged = dict(entity["data"])
        merged.update(patch)
        updated = tx.update_entity(entity["id"], expected, next_status, merged)
        tx.record_audit(
            entity["id"], actor, request["action"],
            entity["status"], updated["status"], {"patch": patch, "offline": True},
        )
        detail = {}
        if updated["kind"] == "sample" and request["action"] in (
            "lab_result", "retest", "close"
        ):
            bumped = self._bump_observation_version(
                tx, updated["data"].get("observation_id"), actor, request["action"]
            )
            if bumped:
                detail["observation_version"] = bumped["version"]
        elif updated["kind"] == "cluster" and request["action"] == "confirm_cluster":
            for observation_id in updated["data"].get("observation_ids", []):
                self._bump_observation_version(tx, observation_id, actor, "confirm_cluster")
        return {"status": "applied", "entity_id": updated["id"],
                "result": updated, "detail": detail}

    # ------------------------------------------------------------------
    # Downstream cascades
    # ------------------------------------------------------------------
    def _linked_samples(self, tx, observation_id):
        return self._lookup(tx, "sample", "observation_id", observation_id)

    def _invalidate_sample_result(self, tx, actor, sample, reason):
        old_result = {field: sample["data"].get(field) for field in _LAB_RESULT_FIELDS}
        stripped = {
            key: value for key, value in sample["data"].items()
            if key not in _LAB_RESULT_FIELDS
        }
        next_status, _ = self.rules.validate_transition(
            actor, sample, "invalidate_result", {},
            lambda k, f, v: self._lookup(tx, k, f, v),
        )
        updated = tx.update_entity(sample["id"], sample["version"], next_status, stripped)
        tx.record_audit(
            sample["id"], actor, "invalidate_result",
            sample["status"], updated["status"],
            {"reason": reason, "old_result": old_result},
        )
        return {"sample_id": sample["id"], "old_result": old_result,
                "status": updated["status"], "data": updated["data"]}

    def _confirmed_clusters(self, tx, observation_id):
        clusters = []
        for cluster in tx.list_entities(kind="cluster", status="confirmed"):
            if observation_id in (cluster["data"].get("observation_ids") or []):
                clusters.append(cluster)
        return clusters

    def _recompute_cluster(self, tx, actor, cluster, changed_observation):
        member_ids = cluster["data"].get("observation_ids") or []
        members = [tx.get_entity(member_id) for member_id in member_ids]
        members = [member for member in members if member]
        points = [member["data"] for member in members]
        still_cluster = len(points) >= 3 and _members_still_cluster(points)

        if still_cluster:
            centroid = _centroid(points)
            next_status, _ = self.rules.validate_transition(
                actor, cluster, "recompute_cluster", {},
                lambda k, f, v: self._lookup(tx, k, f, v),
            )
            data = dict(cluster["data"])
            old_centroid = data.get("centroid")
            data["centroid"] = centroid
            updated = tx.update_entity(cluster["id"], cluster["version"], next_status, data)
            tx.record_audit(
                cluster["id"], actor, "recompute_cluster",
                cluster["status"], updated["status"],
                {"old_centroid": old_centroid, "centroid": centroid,
                 "reason": "observation_amend"},
            )
            return {"cluster_id": cluster["id"], "outcome": "recomputed",
                    "centroid": centroid}

        next_status, _ = self.rules.validate_transition(
            actor, cluster, "invalidate_cluster", {},
            lambda k, f, v: self._lookup(tx, k, f, v),
        )
        old_data = {"centroid": cluster["data"].get("centroid"),
                    "observation_ids": cluster["data"].get("observation_ids")}
        updated = tx.update_entity(cluster["id"], cluster["version"], next_status, cluster["data"])
        tx.record_audit(
            cluster["id"], actor, "invalidate_cluster",
            cluster["status"], updated["status"],
            {"reason": "observation_amend", "old": old_data},
        )
        return {"cluster_id": cluster["id"], "outcome": "invalidated"}

    def _bump_observation_version(self, tx, observation_id, actor, reason):
        if not observation_id:
            return None
        observation = tx.get_entity(observation_id)
        if not observation:
            return None
        bumped = tx.update_entity(
            observation_id, observation["version"], observation["status"], observation["data"]
        )
        tx.record_audit(
            observation_id, actor, "downstream_update",
            observation["status"], bumped["status"],
            {"reason": reason, "version": bumped["version"]},
        )
        return bumped


def _centroid(points):
    lat = round(sum(float(point["lat"]) for point in points) / len(points), 6)
    lon = round(sum(float(point["lon"]) for point in points) / len(points), 6)
    return [lat, lon]


def _members_still_cluster(points, max_days=14, radius_km=10):
    """Whether *all* current members still satisfy the cluster rule.

    Any triple that no longer fits the time window / radius invalidates the
    aggregate conclusion.
    """
    for i, anchor in enumerate(points):
        for other in points[i + 1:]:
            if abs(_date_ordinal(anchor.get("observed_at"))
                   - _date_ordinal(other.get("observed_at"))) > max_days:
                return False
            if _haversine_km(anchor["lat"], anchor["lon"], other["lat"], other["lon"]) > radius_km:
                return False
    return True

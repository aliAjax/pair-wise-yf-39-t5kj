"""Resumable offline batch processing.

Rangers register observations and samples offline and upload them in batches
once back at a station.  Each item carries a stable client-generated event
number (``item_id``) that doubles as an idempotency key.  Batch progress is
persisted in SQLite so a retry after a network outage -- or even a server
restart -- continues from the last confirmed item instead of duplicating work.

When an observation's core fields (species / location / observed_at / lat /
lon) are submitted against an entity that already has finalised downstream
conclusions (a lab result or a confirmed cluster), the change is refused as a
conflict: the stale values are returned but the downstream conclusions are
never overwritten.  When downstream conclusions are not finalised, the change
is applied, the derived sample results and cluster events are queued for
recomputation, and the previous values are kept in the audit trail.
"""

from .audit import AuditTrail
from .domain import (
    ConflictError,
    DomainError,
    NotFoundError,
    ValidationError,
)
from .rules import RuleEngine, is_cluster


# Fields that downstream conclusions (lab results, clusters) depend on.
OBSERVATION_CORE_FIELDS = ("species", "location", "observed_at", "lat", "lon")

# Downstream statuses that count as "finalised" -- they must not be silently
# overwritten by a stale offline update.
FINALISED_SAMPLE_STATUSES = ("resulted", "closed")
FINALISED_CLUSTER_STATUSES = ("confirmed",)

# Item statuses.
STATUS_PENDING = "pending"
STATUS_CONFIRMED = "confirmed"
STATUS_CONFLICT = "conflict"
STATUS_FAILED = "failed"

# Batch statuses.
BATCH_PENDING = "pending"
BATCH_IN_PROGRESS = "in_progress"
BATCH_COMPLETED = "completed"
BATCH_COMPLETED_CONFLICTS = "completed_with_conflicts"
BATCH_PARTIAL = "partial"


class OfflineConflictError(ConflictError):
    """A stale offline update conflicts with finalised downstream conclusions."""

    def __init__(self, message, details=None):
        super().__init__(message)
        self.details = details or {}


class OfflineService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    # ------------------------------------------------------------------
    # Batch submission / resume
    # ------------------------------------------------------------------

    def submit_batch(self, actor, batch_id, items, device_id=None):
        """Persist and process a batch, resuming from confirmed items.

        ``items`` is a list of dicts with at least ``item_id`` and ``op``.
        Returns the batch record including every item's per-item result.
        """
        if not batch_id:
            raise ValidationError("batch_id is required")
        if not isinstance(items, list) or not items:
            raise ValidationError("items must be a non-empty list")

        batch = self.repository.create_offline_batch(batch_id, device_id or "", actor.user_id)
        self.repository.update_offline_batch_status(batch_id, BATCH_IN_PROGRESS)

        # Upsert items in stable order.  Already-known items keep their status
        # so a retry never reprocesses a confirmed item.
        for seq, raw in enumerate(items):
            item_id = raw.get("item_id")
            if not item_id:
                raise ValidationError("item_id is required for every batch item")
            op = raw.get("op")
            if op not in ("create", "transition"):
                raise ValidationError("op must be create or transition for item " + str(item_id))
            self.repository.upsert_offline_batch_item(
                batch_id=batch_id,
                item_id=item_id,
                seq=seq,
                op=op,
                kind=raw.get("kind"),
                payload=raw,
            )

        stored = self.repository.list_offline_batch_items(batch_id)
        for item in stored:
            if item["status"] == STATUS_CONFIRMED:
                continue  # resume from the confirmed point
            self._process_item(actor, batch, item)

        return self.get_batch(batch_id)

    def get_batch(self, batch_id):
        batch = self.repository.get_offline_batch(batch_id)
        if not batch:
            raise NotFoundError("batch not found: " + batch_id)
        items = self.repository.list_offline_batch_items(batch_id)
        statuses = {item["status"] for item in items}
        if statuses and statuses <= {STATUS_CONFIRMED}:
            batch_status = BATCH_COMPLETED
        elif any(s == STATUS_FAILED for s in statuses):
            batch_status = BATCH_PARTIAL
        elif any(s == STATUS_CONFLICT for s in statuses):
            batch_status = BATCH_COMPLETED_CONFLICTS
        else:
            batch_status = BATCH_IN_PROGRESS
        if batch["status"] != batch_status:
            self.repository.update_offline_batch_status(batch_id, batch_status)
            batch = self.repository.get_offline_batch(batch_id)
        batch["items"] = items
        return batch

    # ------------------------------------------------------------------
    # Item processing
    # ------------------------------------------------------------------

    def _process_item(self, actor, batch, item):
        batch_id = batch["batch_id"]
        item_id = item["item_id"]
        payload = item["payload"]
        try:
            if item["op"] == "create":
                result = self._process_create(actor, batch_id, item_id, payload)
            else:
                result = self._process_transition(actor, batch_id, item_id, payload)
            self.repository.update_offline_batch_item(
                batch_id, item_id, STATUS_CONFIRMED, result=result
            )
        except OfflineConflictError as exc:
            self.repository.update_offline_batch_item(
                batch_id,
                item_id,
                STATUS_CONFLICT,
                error={
                    "type": "OfflineConflictError",
                    "message": str(exc),
                    "details": exc.details,
                },
            )
        except ConflictError as exc:
            self.repository.update_offline_batch_item(
                batch_id,
                item_id,
                STATUS_CONFLICT,
                error={"type": type(exc).__name__, "message": str(exc)},
            )
        except DomainError as exc:
            self.repository.update_offline_batch_item(
                batch_id,
                item_id,
                STATUS_FAILED,
                error={"type": type(exc).__name__, "message": str(exc)},
            )

    def _idem_key(self, batch_id, item_id):
        return "%s:%s" % (batch_id, item_id)

    def _process_create(self, actor, batch_id, item_id, payload):
        kind = self.rules.normalize_kind(payload.get("kind"))
        data = self._resolve_data_refs(dict(payload.get("data") or {}), batch_id)
        idem_key = self._idem_key(batch_id, item_id)

        if kind == "observation":
            return self._create_observation(actor, batch_id, item_id, data, idem_key)

        # Idempotent create for every other kind.
        return self._idempotent_create(actor, kind, data, idem_key)

    def _idempotent_create(self, actor, kind, data, idem_key):
        existing = self.repository.get_idempotency(actor.user_id, idem_key)
        if existing:
            entity = self.repository.get_entity(existing)
            if entity:
                return self._entity_ref(entity)
        self.rules.validate_create(actor, kind, data, self._lookup)
        entity_id = str(data.pop("id", "") or self._new_id())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind)
        entity = self.repository.create_entity(entity_id, kind, status, data, actor.user_id)
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        self.repository.save_idempotency(actor.user_id, idem_key, entity_id)
        return self._entity_ref(entity)

    def _create_observation(self, actor, batch_id, item_id, data, idem_key):
        event_id = data.get("event_id")
        existing = self._find_observation_by_event_id(event_id)

        if not existing:
            return self._idempotent_create(actor, "observation", data, idem_key)

        # The observation already exists.  Compare the core fields the device
        # is submitting against what the server currently holds.
        changed = self._changed_core(existing, data, OBSERVATION_CORE_FIELDS)
        if not changed:
            # Identical resubmission: idempotent success, no new entity/audit.
            return self._entity_ref(existing)

        # Core fields differ.  Finalised downstream conclusions must block the
        # change; otherwise apply it and queue recomputation.
        updated = self._apply_observation_core(
            actor,
            existing,
            data,
            source="offline_batch:%s:%s" % (batch_id, item_id),
        )
        return self._entity_ref(updated)

    def _process_transition(self, actor, batch_id, item_id, payload):
        entity_ref = payload.get("entity_ref")
        action = payload.get("action")
        data = dict(payload.get("data") or {})
        expected_version = payload.get("expected_version")

        entity = self._resolve_entity_ref(entity_ref, actor, batch_id)
        if not entity:
            raise NotFoundError("entity not found for ref: " + str(entity_ref))

        kind = self.rules.normalize_kind(entity["kind"])

        # Idempotent transition: if the entity is already in the target status
        # (or beyond it), treat the action as a no-op success so a retry does
        # not raise InvalidTransition.
        transition = self.rules.TRANSITIONS.get(kind, {}).get(action)
        if transition and entity["status"] == transition[1]:
            return self._entity_ref(entity)

        next_status, patch = self.rules.validate_transition(
            actor, entity, action, data, self._lookup
        )
        merged = dict(entity["data"])
        merged.update(patch)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        updated = self.repository.update_entity(entity["id"], expected, next_status, merged)
        self.audit.record(
            entity["id"],
            actor,
            action,
            entity["status"],
            updated["status"],
            {"patch": patch, "source": "offline_batch:%s:%s" % (batch_id, item_id)},
        )

        # A sample change (e.g. a lab result) can invalidate cluster membership
        # that depends on the linked observation.
        if kind == "sample" and updated["status"] != entity["status"]:
            self._queue_cluster_recompute_for_sample(updated)

        return self._entity_ref(updated)

    # ------------------------------------------------------------------
    # Observation core-field guard + invalidation
    # ------------------------------------------------------------------

    def _apply_observation_core(self, actor, observation, patch, source):
        """Apply core-field changes to an observation.

        Raises ``OfflineConflictError`` when finalised downstream conclusions
        exist.  Otherwise applies the change, queues recomputation of the
        derived sample results and cluster events, and records the previous
        values in the audit trail.
        """
        changed = self._changed_core(observation, patch, OBSERVATION_CORE_FIELDS)
        if not changed:
            return observation

        downstream = self._observation_downstream(observation["id"])
        if downstream["finalised"]:
            raise OfflineConflictError(
                "observation %s has finalised downstream conclusions; "
                "stale core fields cannot overwrite them" % observation["id"],
                details={
                    "observation_id": observation["id"],
                    "event_id": observation["data"].get("event_id"),
                    "submitted": {f: changed[f][1] for f in changed},
                    "current": {f: changed[f][0] for f in changed},
                    "blocking_samples": [
                        {"id": s["id"], "status": s["status"], "result": s["data"].get("result")}
                        for s in downstream["samples"]
                        if s["status"] in FINALISED_SAMPLE_STATUSES
                    ],
                    "blocking_clusters": [
                        {"id": c["id"], "status": c["status"]}
                        for c in downstream["clusters"]
                        if c["status"] in FINALISED_CLUSTER_STATUSES
                    ],
                },
            )

        merged = dict(observation["data"])
        merged.update({f: changed[f][1] for f in changed})
        updated = self.repository.update_entity(
            observation["id"], observation["version"], observation["status"], merged
        )
        self.audit.record(
            observation["id"],
            actor,
            "offline_change",
            observation["status"],
            observation["status"],
            {
                "source": source,
                "old": {f: changed[f][0] for f in changed},
                "new": {f: changed[f][1] for f in changed},
            },
        )

        # Invalidate derived conclusions; recomputation happens later.
        for sample in downstream["samples"]:
            self.repository.enqueue_recompute(
                sample["id"], "sample", "observation_changed", observation["id"]
            )
        for cluster in downstream["clusters"]:
            self.repository.enqueue_recompute(
                cluster["id"], "cluster", "observation_changed", observation["id"]
            )
        return updated

    def _observation_downstream(self, observation_id):
        samples = [
            s
            for s in self.repository.list_entities(kind="sample")
            if s["data"].get("observation_id") == observation_id
        ]
        clusters = [
            c
            for c in self.repository.list_entities(kind="cluster")
            if observation_id in (c["data"].get("observation_ids") or [])
        ]
        finalised = any(s["status"] in FINALISED_SAMPLE_STATUSES for s in samples) or any(
            c["status"] in FINALISED_CLUSTER_STATUSES for c in clusters
        )
        return {"samples": samples, "clusters": clusters, "finalised": finalised}

    def _queue_cluster_recompute_for_sample(self, sample):
        """Queue recomputation of clusters containing the sample's observation."""
        observation_id = sample["data"].get("observation_id")
        if not observation_id:
            return
        for cluster in self.repository.list_entities(kind="cluster"):
            if observation_id in (cluster["data"].get("observation_ids") or []):
                self.repository.enqueue_recompute(
                    cluster["id"],
                    "cluster",
                    "sample_changed",
                    sample["id"],
                )

    # ------------------------------------------------------------------
    # Recomputation
    # ------------------------------------------------------------------

    def recompute_pending(self, actor=None):
        """Process the recompute queue and return per-item results."""
        results = []
        for item in self.repository.list_pending_recompute():
            if item["kind"] == "cluster":
                result = self._recompute_cluster(item, actor)
            elif item["kind"] == "sample":
                result = self._recompute_sample(item, actor)
            else:
                result = {"skipped": True, "kind": item["kind"]}
            self.repository.mark_recompute_done(item["id"])
            results.append({"queue_id": item["id"], **result})
        return results

    def _recompute_cluster(self, item, actor):
        cluster = self.repository.get_entity(item["entity_id"])
        if not cluster:
            return {"entity_id": item["entity_id"], "recomputed": False, "reason": "missing"}
        obs_ids = cluster["data"].get("observation_ids") or []
        observations = [
            o for o in (self.repository.get_entity(oid) for oid in obs_ids) if o is not None
        ]
        still = is_cluster(
            [
                {
                    "observed_at": o["data"].get("observed_at"),
                    "lat": o["data"].get("lat"),
                    "lon": o["data"].get("lon"),
                }
                for o in observations
            ]
        )
        data = dict(cluster["data"])
        data["recomputed_at"] = self._now()
        if still:
            new_status = cluster["status"]
            action = "recompute_cluster_still_valid"
        else:
            new_status = "draft"
            action = "recompute_cluster_downgraded"
        self.repository.update_entity(cluster["id"], cluster["version"], new_status, data)
        if actor:
            self.audit.record(
                cluster["id"],
                actor,
                action,
                cluster["status"],
                new_status,
                {"reason": item["reason"], "ref": item["ref_entity_id"], "still_cluster": still},
            )
        return {
            "entity_id": cluster["id"],
            "kind": "cluster",
            "recomputed": True,
            "still_cluster": still,
            "status": new_status,
        }

    def _recompute_sample(self, item, actor):
        sample = self.repository.get_entity(item["entity_id"])
        if not sample:
            return {"entity_id": item["entity_id"], "recomputed": False, "reason": "missing"}
        observation_id = sample["data"].get("observation_id")
        observation = self.repository.get_entity(observation_id) if observation_id else None
        link_valid = bool(
            observation and observation["status"] in ("submitted", "sampled")
        )
        data = dict(sample["data"])
        data["recomputed_at"] = self._now()
        data["link_valid"] = link_valid
        updated = self.repository.update_entity(
            sample["id"], sample["version"], sample["status"], data
        )
        if actor:
            self.audit.record(
                sample["id"],
                actor,
                "recompute_sample",
                sample["status"],
                updated["status"],
                {"reason": item["reason"], "ref": item["ref_entity_id"], "link_valid": link_valid},
            )
        return {
            "entity_id": sample["id"],
            "kind": "sample",
            "recomputed": True,
            "link_valid": link_valid,
            "status": updated["status"],
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def _find_observation_by_event_id(self, event_id):
        if not event_id:
            return None
        rows = self._lookup("observation", "event_id", event_id)
        return rows[0] if rows else None

    @staticmethod
    def _changed_core(entity, patch, fields):
        """Return {field: (old, new)} for core fields that actually change."""
        current = entity.get("data", {})
        changed = {}
        for field in fields:
            if field not in patch:
                continue
            old = current.get(field)
            new = patch[field]
            if old != new:
                changed[field] = (old, new)
        return changed

    def _resolve_entity_ref(self, entity_ref, actor, batch_id):
        if not entity_ref:
            return None
        if isinstance(entity_ref, str) and entity_ref.startswith("@"):
            ref_item_id = entity_ref[1:]
            ref = self.repository.get_offline_batch_item(batch_id, ref_item_id)
            if not ref or ref["status"] != STATUS_CONFIRMED or not ref["result"]:
                raise NotFoundError(
                    "referenced batch item %s is not confirmed yet" % ref_item_id
                )
            return self.repository.get_entity(ref["result"]["entity_id"])
        return self.repository.get_entity(str(entity_ref))

    def _resolve_data_refs(self, value, batch_id):
        """Recursively resolve ``@item_id`` references inside a create payload."""
        if isinstance(value, dict):
            return {k: self._resolve_data_refs(v, batch_id) for k, v in value.items()}
        if isinstance(value, list):
            return [self._resolve_data_refs(v, batch_id) for v in value]
        if isinstance(value, str) and value.startswith("@"):
            ref = self.repository.get_offline_batch_item(batch_id, value[1:])
            if ref and ref["status"] == STATUS_CONFIRMED and ref["result"]:
                return ref["result"]["entity_id"]
        return value

    @staticmethod
    def _entity_ref(entity):
        return {
            "entity_id": entity["id"],
            "kind": entity["kind"],
            "status": entity["status"],
            "version": entity["version"],
        }

    @staticmethod
    def _new_id():
        from uuid import uuid4

        return str(uuid4())

    @staticmethod
    def _now():
        from datetime import datetime, timezone

        return datetime.now(timezone.utc).isoformat(timespec="seconds")

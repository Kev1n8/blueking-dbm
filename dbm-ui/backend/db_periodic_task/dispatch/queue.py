# -*- coding: utf-8 -*-
"""
TencentBlueKing is pleased to support the open source community by making 蓝鲸智云-DB管理系统(BlueKing-BK-DBM) available.
Copyright (C) 2017-2023 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at https://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
"""

from __future__ import annotations

import json
import logging
import time
from typing import ClassVar, Optional

from django.utils.module_loading import autodiscover_modules
from redis import Redis

from backend.db_periodic_task.dispatch import routing
from backend.db_periodic_task.dispatch.config import (
    DEFAULT_QUEUE_WAIT_TTL_SECONDS,
    DispatchQueueConfig,
    DispatchTaskConfig,
)
from backend.db_periodic_task.dispatch.job import DispatchJob, build_job_id
from backend.db_periodic_task.dispatch.metrics import METRICS_WINDOW_SECONDS, DispatchMetrics
from backend.db_periodic_task.dispatch.outcomes import DispatchOutcomeType

logger = logging.getLogger("root")

DEFAULT_NAMESPACE = "default"
# Rebuildable membership-hash safety net — independent of per-task wait TTL.
# Maintenance rebuilds the hash when it expires ahead of long-lived jobs.
TASK_MEMBERS_CACHE_TTL_SECONDS = DEFAULT_QUEUE_WAIT_TTL_SECONDS

# ``dispatch:registered`` is the only namespace-agnostic dispatch key.
KEY_REGISTERED = "dispatch:registered"

# Namespace-scoped key families. Everything namespace-derivable travels
# together as ``dispatch:{ns}:*`` so one glob sweeps a whole shard.
KEY_JOB_PREFIX = "dispatch:{ns}:job:"
KEY_DEDUPE_PREFIX = "dispatch:{ns}:dedupe:"
KEY_PRODUCER_LOCK_PREFIX = "dispatch:{ns}:producer_lock"


def set_redis_ttl_marker(key: str, ttl_seconds: int, *, nx: bool = False, client=None) -> bool:
    """SET ``key`` with a TTL.

    With ``nx=True``, acts as a gate: returns True only when the key was absent
    (first caller in the window wins). With ``nx=False``, refreshes the marker.
    """
    if client is None:
        raise TypeError("set_redis_ttl_marker requires an explicit client (routing must stay explicit)")
    try:
        ttl = max(1, int(ttl_seconds))
        if nx:
            return bool(client.set(key, "1", nx=True, ex=ttl))
        client.set(key, "1", ex=ttl)
        return True
    except Exception as exc:
        logger.warning("dispatch: set ttl marker key=%s nx=%s failed: %s", key, nx, exc)
        return False


class DispatchQueueError(Exception):
    """Raised for an invalid queue registration.

    The only trigger today is two concrete ``DispatchQueue`` subclasses claiming
    the same non-empty ``namespace``. The conflict is detected at *class
    definition time* (the moment the offending module is imported) so it surfaces
    loudly and early, instead of silently letting the first-discovered queue win.
    """


# Registry of concrete queue subclasses keyed by their ``namespace``.
# Populated automatically via ``DispatchQueue.__init_subclass__``. Every
# ``namespace`` must be unique across all apps that ship a ``dispatch_queues``
# module; a duplicate raises ``DispatchQueueError`` (see ``__init_subclass__``).
DISPATCH_QUEUE_REGISTRY: dict[str, type["DispatchQueue"]] = {}

_queues_discovered = False


class _ConfigNamespace:
    """Class-level ``namespace`` resolved from ``config_cls``."""

    def __get__(self, instance, owner: Optional[type["DispatchQueue"]]) -> str:
        if owner is None:
            return ""
        return getattr(owner.config_cls, "namespace", "") or ""


class DispatchQueue:
    """Abstract Redis-backed dispatch queue.

    This base class is not tied to a task type. Each subclass declares its
    namespace and dequeue policy through ``config_cls``.

    Example::

        class AITaskQueue(DispatchQueue):
            config_cls = AITaskQueueConfig

    Job data, outcomes, and queue operations bind to ``cls.namespace`` and
    travel together on that namespace's Redis shard; only registration
    metadata (``dispatch:registered``) stays global. Cross-queue helpers
    iterate queues registered in ``DISPATCH_QUEUE_REGISTRY``.

    Registration contract
    ----------------------
    * A subclass is auto-registered when its module is imported (see
      ``__init_subclass__``). To make a queue discoverable, ship a
      ``dispatch_queues.py`` in an ``INSTALLED_APPS`` entry that imports the
      queue class. The file name is a hard requirement — the dispatcher calls
      ``django.utils.module_loading.autodiscover_modules("dispatch_queues")``.
    * ``namespace`` (from ``config_cls``) MUST be non-empty **and unique** across
      every queue in the project. Declaring the same namespace in two queues
      raises ``DispatchQueueError`` at import time — there is no silent
      first-wins fallback anymore. Pick a namespace before writing the queue and
      grep existing ``DispatchQueueConfig`` subclasses to avoid collisions.

    This class owns registration, key layout, job storage, config resolution,
    counts, and outcome metadata. Mechanical queue operations live next door:
    admission (``QueueAdmission``), reservation (``QueueReservation``),
    finalize/requeue (``QueueLifecycle``), orphan reaping (``OrphanReaper``),
    and derived task-member counts (``TaskMembers``) — each helper takes the
    queue class as its first argument.
    """

    config_cls: ClassVar[type[DispatchQueueConfig]] = DispatchQueueConfig
    namespace = _ConfigNamespace()

    def __init_subclass__(cls, _ephemeral: bool = False, **kwargs):
        super().__init_subclass__(**kwargs)
        # Only concrete queues with a non-empty namespace are registered; the
        # abstract base (namespace == "") is skipped.
        if not cls.namespace:
            return
        try:
            routing.validate_namespace(cls.namespace)
        except Exception as exc:
            raise DispatchQueueError(f"invalid dispatch queue namespace {cls.namespace!r}: {exc}") from exc
        incumbent = DISPATCH_QUEUE_REGISTRY.get(cls.namespace)
        # Same class re-registered (idempotent import): keep as-is.
        if incumbent is None or incumbent is cls:
            DISPATCH_QUEUE_REGISTRY[cls.namespace] = cls
            return
        if _ephemeral:
            # Ephemeral throwaway queues must never clobber a real registered
            # queue, and must never raise. Only a stale ephemeral (one created
            # earlier for the same namespace) is replaced.
            if incumbent.__qualname__.startswith("EphemeralQueue"):
                DISPATCH_QUEUE_REGISTRY[cls.namespace] = cls
            return
        # Two *real* queues claim the same namespace: fail fast at import time.
        # ``_ephemeral`` is only ever passed by ``ephemeral_queue_for_namespace``
        # (always False for source-defined subclasses), so this fires exactly
        # when two apps ship a ``dispatch_queues`` module that registers queues
        # with identical namespaces.
        raise DispatchQueueError(
            f"duplicate dispatch queue namespace {cls.namespace!r}: "
            f"{cls.__qualname__} conflicts with already-registered "
            f"{incumbent.__qualname__}"
        )

    # ------------------------------------------------------------------ #
    # Registry / cross-queue helpers (base-level, operate over all queues)
    # ------------------------------------------------------------------ #
    @classmethod
    def ensure_queues_loaded(cls) -> None:
        """Import each installed app's optional ``dispatch_queues`` module once."""
        global _queues_discovered
        if _queues_discovered:
            return
        autodiscover_modules("dispatch_queues")
        _queues_discovered = True

    @classmethod
    def iter_queues(cls) -> list[type["DispatchQueue"]]:
        cls.ensure_queues_loaded()
        return list(DISPATCH_QUEUE_REGISTRY.values())

    @classmethod
    def queue_for_namespace(cls, namespace: str) -> Optional[type["DispatchQueue"]]:
        cls.ensure_queues_loaded()
        return DISPATCH_QUEUE_REGISTRY.get(namespace or DEFAULT_NAMESPACE)

    @classmethod
    def ephemeral_queue_for_namespace(cls, namespace: str) -> type["DispatchQueue"]:
        """Throwaway base queue bound to ``namespace`` for cleanup paths.

        Used when no concrete queue is registered for the namespace (owning
        module removed or discovery failed) so namespace-derived keys still
        target where jobs actually live instead of the default namespace. The
        class is never left in ``DISPATCH_QUEUE_REGISTRY``: ``iter_queues()``
        must not start pumping a namespace whose owning module is gone.
        """
        config_cls = type(f"EphemeralQueueConfig:{namespace}", (DispatchQueueConfig,), {"namespace": namespace})
        # ``_ephemeral=True`` is forwarded by ``type(...)`` to ``__init_subclass__``
        # so this throwaway queue registers without ever raising on a namespace
        # that a real queue already owns.
        queue_cls = type(f"EphemeralQueue:{namespace}", (cls,), {"config_cls": config_cls}, _ephemeral=True)
        # ``__init_subclass__`` registers any non-empty namespace; drop the
        # throwaway unless a real queue won the race and stayed registered.
        if DISPATCH_QUEUE_REGISTRY.get(namespace) is queue_cls:
            DISPATCH_QUEUE_REGISTRY.pop(namespace, None)
        return queue_cls

    # ------------------------------------------------------------------ #
    # Namespace-bound configuration / key layout
    # ------------------------------------------------------------------ #
    @classmethod
    def _ns(cls) -> str:
        return cls.namespace or DEFAULT_NAMESPACE

    @classmethod
    def conn(cls) -> Redis[str]:
        """The Redis connection owning this namespace's dispatch keys."""
        return routing.conn_for_namespace(cls._ns())

    @classmethod
    def load_config(cls) -> DispatchQueueConfig:
        return cls.config_cls.from_db()

    @classmethod
    def pending_key(cls) -> str:
        return f"dispatch:{cls._ns()}:pending"

    @classmethod
    def inflight_key(cls) -> str:
        return f"dispatch:{cls._ns()}:inflight"

    @classmethod
    def dedupe_key(cls, task_key: str, work_item_id: str) -> str:
        return f"{KEY_DEDUPE_PREFIX.format(ns=cls._ns())}{task_key}:{work_item_id}"

    @classmethod
    def dedupe_key_for_namespace(cls, namespace: str, task_key: str, work_item_id: str) -> str:
        return f"{KEY_DEDUPE_PREFIX.format(ns=namespace or DEFAULT_NAMESPACE)}{task_key}:{work_item_id}"

    @classmethod
    def tick_counter_key(cls, tick_id: int) -> str:
        return f"dispatch:{cls._ns()}:tick:{int(tick_id)}"

    @classmethod
    def task_members_key(cls) -> str:
        return f"dispatch:{cls._ns()}:task_members"

    @classmethod
    def producer_lock_key(cls) -> str:
        """Producer-gate key; admission rejects ``submit`` while it exists."""
        return KEY_PRODUCER_LOCK_PREFIX.format(ns=cls._ns())

    @classmethod
    def _pending_member_field(cls, task_key: str) -> str:
        return f"pending:{task_key}"

    @classmethod
    def _inflight_member_field(cls, task_key: str) -> str:
        return f"inflight:{task_key}"

    @classmethod
    def _member_count_for_task(cls, field: str) -> int:
        try:
            return int(cls.conn().hget(cls.task_members_key(), field) or 0)
        except Exception as exc:
            logger.warning("dispatch: hget task_members field=%s failed: %s", field, exc)
            return -1

    # ------------------------------------------------------------------ #
    # Namespace-scoped job-record storage
    # ------------------------------------------------------------------ #
    @classmethod
    def _job_key(cls, job_id: str) -> str:
        return f"{KEY_JOB_PREFIX.format(ns=cls._ns())}{job_id}"

    @classmethod
    def get_job(cls, job_id: str) -> Optional[DispatchJob]:
        try:
            raw = cls.conn().get(cls._job_key(job_id))
            if not raw:
                return None
            return DispatchJob.from_dict(json.loads(raw))
        except Exception as exc:
            logger.warning("dispatch: get_job failed job_id=%s: %s", job_id, exc)
            return None

    @classmethod
    def get_jobs(cls, job_ids: list[str]) -> dict[str, DispatchJob]:
        """Load a candidate batch in one Redis round trip."""
        if not job_ids:
            return {}
        try:
            raw_jobs = cls.conn().mget([cls._job_key(job_id) for job_id in job_ids])
        except Exception as exc:
            logger.warning("dispatch: get_jobs failed namespace=%s: %s", cls._ns(), exc)
            return {}
        jobs = {}
        for job_id, raw in zip(job_ids, raw_jobs):
            if not raw:
                continue
            try:
                jobs[job_id] = DispatchJob.from_dict(json.loads(raw))
            except (TypeError, ValueError, KeyError) as exc:
                logger.warning("dispatch: invalid job payload job_id=%s: %s", job_id, exc)
        return jobs

    # ------------------------------------------------------------------ #
    # Dequeue eligibility
    # ------------------------------------------------------------------ #
    @classmethod
    def is_congestion_outcome(cls, outcome: DispatchOutcomeType) -> bool:
        """Whether ``outcome`` should feed namespace AIMD congestion feedback."""
        return False

    @classmethod
    def peek_eligible(cls, limit: int, now: Optional[float] = None) -> list[str]:
        """Read ready candidates without changing queue membership."""
        try:
            now = now or time.time()
            raw_ids = cls.conn().zrangebyscore(
                cls.pending_key(),
                "-inf",
                now,
                start=0,
                num=max(1, int(limit)),
            )
            return [job_id.decode() if isinstance(job_id, bytes) else job_id for job_id in (raw_ids or [])]
        except Exception as exc:
            logger.warning("dispatch: peek_eligible failed namespace=%s: %s", cls._ns(), exc)
            return []

    # ------------------------------------------------------------------ #
    # Config resolution helpers keyed off the stored job (global)
    # ------------------------------------------------------------------ #
    @classmethod
    def config_for_job(cls, job: DispatchJob) -> Optional[DispatchTaskConfig]:
        """Resolve the effective config for a stored job.

        A non-empty ``config_json`` is a frozen snapshot (submit() was given
        explicit ``config`` overrides) and wins. Otherwise resolve the live DB
        config for the task_key, so jobs need not duplicate config per record.
        Returns ``None`` when the task is unregistered or resolution fails.
        """
        from backend.db_periodic_task.dispatch.registry import DISPATCH_REGISTRY

        task_cls = DISPATCH_REGISTRY.get(job.task_key)
        if task_cls is None:
            logger.error("dispatch: config task is not registered task_key=%s job_id=%s", job.task_key, job.job_id)
            return None
        try:
            if job.config_json:
                return task_cls.config_cls.from_json(job.config_json)
            return task_cls.config_cls.from_db()
        except Exception as exc:
            source = "frozen" if job.config_json else "live"
            logger.error(
                "dispatch: %s config resolution failed task_key=%s job_id=%s: %s",
                source,
                job.task_key,
                job.job_id,
                exc,
            )
            return None

    @classmethod
    def resolve_queue_wait_ttl_from_job(cls, job: DispatchJob) -> int:
        config = cls.config_for_job(job)
        return config.resolve_queue_wait_ttl_seconds() if config else DEFAULT_QUEUE_WAIT_TTL_SECONDS

    @classmethod
    def resolve_execution_timeout_from_job(cls, job: DispatchJob) -> int:
        from backend.db_periodic_task.dispatch.config import DEFAULT_EXECUTION_TIMEOUT_SECONDS

        config = cls.config_for_job(job)
        return config.resolve_execution_timeout_seconds() if config else DEFAULT_EXECUTION_TIMEOUT_SECONDS

    @classmethod
    def resolve_inflight_ttl_from_job(cls, job: DispatchJob) -> int:
        from backend.db_periodic_task.dispatch.config import INFLIGHT_TTL_MARGIN_SECONDS

        config = cls.config_for_job(job)
        if config:
            return config.resolve_inflight_ttl_seconds()
        from backend.db_periodic_task.dispatch.config import DEFAULT_EXECUTION_TIMEOUT_SECONDS

        return DEFAULT_EXECUTION_TIMEOUT_SECONDS + INFLIGHT_TTL_MARGIN_SECONDS

    # ------------------------------------------------------------------ #
    # Outcomes / registered metadata
    # ------------------------------------------------------------------ #
    @classmethod
    def record_outcome(
        cls,
        task_key: str,
        outcome: DispatchOutcomeType,
        *,
        elapsed_seconds: float = -1.0,
        completed: bool = True,
        sample_identity: str = "",
    ) -> None:
        DispatchMetrics.record_task_outcome(
            cls._ns(),
            task_key,
            outcome,
            elapsed_seconds=elapsed_seconds,
            completed=completed,
            congested=cls.is_congestion_outcome(outcome),
            sample_identity=sample_identity,
        )

    @classmethod
    def register_task_metadata(cls, task_key: str, metadata: dict) -> None:
        try:
            routing.global_conn().hset(KEY_REGISTERED, task_key, json.dumps(metadata, ensure_ascii=False))
            cls.reconcile_registered_metadata()
        except Exception as exc:
            logger.warning("dispatch: register_task_metadata failed: %s", exc)

    @classmethod
    def reconcile_registered_metadata(cls) -> int:
        """Drop ``dispatch:registered`` fields for unknown tasks or missing queues.

        Called on every task registration so process startup rewrites Redis to match
        the in-memory ``DISPATCH_REGISTRY`` / ``DISPATCH_QUEUE_REGISTRY``.
        """
        from backend.db_periodic_task.dispatch.registry import DISPATCH_REGISTRY

        cls.ensure_queues_loaded()
        known_tasks = set(DISPATCH_REGISTRY)
        known_namespaces = set(DISPATCH_QUEUE_REGISTRY)
        for task_cls in DISPATCH_REGISTRY.values():
            namespace = getattr(task_cls, "namespace", "") or ""
            if namespace:
                known_namespaces.add(namespace)
        try:
            raw = routing.global_conn().hgetall(KEY_REGISTERED) or {}
        except Exception as exc:
            logger.warning("dispatch: reconcile_registered_metadata hgetall failed: %s", exc)
            return 0

        stale: list[str] = []
        for raw_key, raw_value in raw.items():
            field = raw_key.decode() if isinstance(raw_key, bytes) else str(raw_key)
            if field not in known_tasks:
                stale.append(field)
                continue
            try:
                payload = raw_value.decode() if isinstance(raw_value, bytes) else raw_value
                meta = json.loads(payload) if payload else {}
            except (TypeError, ValueError, json.JSONDecodeError):
                stale.append(field)
                continue
            namespace = str(meta.get("namespace") or "")
            if namespace and namespace not in known_namespaces:
                stale.append(field)

        if not stale:
            return 0
        try:
            routing.global_conn().hdel(KEY_REGISTERED, *stale)
        except Exception as exc:
            logger.warning("dispatch: reconcile_registered_metadata hdel failed: %s", exc)
            return 0
        logger.info("dispatch: reconciled registered metadata removed=%s", stale)
        return len(stale)

    @classmethod
    def outcomes_for_task(cls, task_key: str) -> dict[str, int]:
        now = time.time()
        counters = DispatchMetrics.aggregate_task_counters(
            cls._ns(),
            task_key,
            start_at=now - METRICS_WINDOW_SECONDS,
            end_at=now,
        )
        return {
            name.removeprefix("outcome:"): count for name, count in counters.items() if name.startswith("outcome:")
        }

    # ------------------------------------------------------------------ #
    # Namespace-bound counts
    # ------------------------------------------------------------------ #
    @classmethod
    def pending_count(cls) -> int:
        try:
            return int(cls.conn().zcard(cls.pending_key()) or 0)
        except Exception:
            return -1

    @classmethod
    def inflight_count(cls) -> int:
        try:
            return int(cls.conn().zcard(cls.inflight_key()) or 0)
        except Exception:
            return -1

    @classmethod
    def delaying_count(cls, now: Optional[float] = None) -> int:
        try:
            now = now or time.time()
            return int(cls.conn().zcount(cls.pending_key(), now, "+inf") or 0)
        except Exception:
            return -1

    @classmethod
    def ready_count(cls, now: Optional[float] = None) -> int:
        try:
            now = now or time.time()
            return int(cls.conn().zcount(cls.pending_key(), "-inf", now) or 0)
        except Exception:
            return -1

    # ------------------------------------------------------------------ #
    # Cross-queue aggregate counts (base-level, sum over registered queues)
    # ------------------------------------------------------------------ #
    @classmethod
    def aggregate_pending_count(cls) -> int:
        return sum(max(0, q.pending_count()) for q in cls.iter_queues())

    @classmethod
    def aggregate_inflight_count(cls) -> int:
        return sum(max(0, q.inflight_count()) for q in cls.iter_queues())

    @classmethod
    def aggregate_delaying_count(cls, now: Optional[float] = None) -> int:
        return sum(max(0, q.delaying_count(now)) for q in cls.iter_queues())

    @classmethod
    def aggregate_ready_count(cls, now: Optional[float] = None) -> int:
        return sum(max(0, q.ready_count(now)) for q in cls.iter_queues())

    # ------------------------------------------------------------------ #
    # Namespace-bound per-task membership / counts
    # ------------------------------------------------------------------ #
    @classmethod
    def has_pending_for_task(cls, task_key: str) -> bool:
        # ``!= 0`` treats an unreadable count (-1) as busy: fail closed so a
        # Redis blip can never be mistaken for an empty queue.
        return cls.pending_count_for_task(task_key) != 0

    @classmethod
    def has_inflight_for_task(cls, task_key: str) -> bool:
        return cls.inflight_count_for_task(task_key) != 0

    @classmethod
    def pending_count_for_task(cls, task_key: str) -> int:
        return cls._member_count_for_task(cls._pending_member_field(task_key))

    @classmethod
    def inflight_count_for_task(cls, task_key: str) -> int:
        return cls._member_count_for_task(cls._inflight_member_field(task_key))

    @classmethod
    def task_counts(cls, task_key: str) -> tuple[int, int]:
        """Return pending and inflight counts in one Redis round trip."""
        fields = [cls._pending_member_field(task_key), cls._inflight_member_field(task_key)]
        try:
            values = cls.conn().hmget(cls.task_members_key(), fields) or []
            counts = [int(value or 0) for value in values]
            if len(counts) != len(fields):
                return -1, -1
            return counts[0], counts[1]
        except Exception as exc:
            logger.warning("dispatch: hmget task_members task_key=%s failed: %s", task_key, exc)
            return -1, -1

    # ------------------------------------------------------------------ #
    # Job construction
    # ------------------------------------------------------------------ #
    @classmethod
    def new_job(
        cls,
        *,
        task_key: str,
        work_item_id: str,
        work_item_data: dict,
        payload_json: str = "",
        config_json: str = "",
        execute_at: Optional[float] = None,
    ) -> DispatchJob:
        now = time.time()
        job_id = build_job_id(task_key, work_item_id)
        return DispatchJob(
            job_id=job_id,
            task_key=task_key,
            namespace=cls._ns(),
            work_item_id=work_item_id,
            work_item_data=work_item_data,
            payload_json=payload_json,
            config_json=config_json,
            created_at=now,
            execute_at=execute_at if execute_at is not None else now,
        )

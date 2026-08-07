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

import logging
import math
import time
from abc import ABC, abstractmethod
from typing import Any, Optional

from backend.db_periodic_task.dispatch.admission import ADMISSION_BATCH_SIZE, EnqueueStatus, QueueAdmission
from backend.db_periodic_task.dispatch.config import DispatchTaskConfig
from backend.db_periodic_task.dispatch.job import DispatchJob
from backend.db_periodic_task.dispatch.lifecycle import QueueLifecycle
from backend.db_periodic_task.dispatch.metrics import DispatchMetrics
from backend.db_periodic_task.dispatch.outcomes import DispatchOutcome, DispatchOutcomeType
from backend.db_periodic_task.dispatch.queue import DispatchQueue
from backend.db_periodic_task.dispatch.reaper import OrphanReaper
from backend.db_periodic_task.dispatch.routing import candidate_namespaces, namespace_for_job_id
from backend.db_periodic_task.dispatch.scheduling import ExecuteAtSpec, resolve_execute_at

logger = logging.getLogger("root")

_ENQUEUE_OUTCOME_BY_STATUS = {
    EnqueueStatus.ACCEPTED: DispatchOutcomeType.ENQUEUED,
    EnqueueStatus.DUPLICATE: DispatchOutcomeType.ENQUEUE_DUPLICATE,
    EnqueueStatus.CAPACITY_REJECTED: DispatchOutcomeType.ENQUEUE_CAPACITY_REJECTED,
    EnqueueStatus.DEADLINE_EXPIRED: DispatchOutcomeType.ENQUEUE_DEADLINE_EXPIRED,
    EnqueueStatus.PRODUCER_REJECTED: DispatchOutcomeType.ENQUEUE_PRODUCER_REJECTED,
    EnqueueStatus.UNAVAILABLE: DispatchOutcomeType.ENQUEUE_UNAVAILABLE,
}
_ENQUEUE_METRIC_BY_OUTCOME = {
    DispatchOutcomeType.ENQUEUED: "enqueued",
    DispatchOutcomeType.ENQUEUE_DUPLICATE: "enqueue_duplicate",
    DispatchOutcomeType.ENQUEUE_CAPACITY_REJECTED: "enqueue_capacity_rejected",
    DispatchOutcomeType.ENQUEUE_DEADLINE_EXPIRED: "enqueue_deadline_expired",
    DispatchOutcomeType.ENQUEUE_PRODUCER_REJECTED: "enqueue_producer_rejected",
    DispatchOutcomeType.ENQUEUE_UNAVAILABLE: "enqueue_unavailable",
}


class DispatchTask(ABC):
    """Base class for a registered dispatch consumer task.

    Extension guide:
    1. Subclass ``DispatchTask`` and implement ``execute`` plus ``_build_job``.
    2. Register with ``@register_dispatch_task(...)``.
    3. Submit work via ``submit(items)``; observe queue state via
       ``pendings`` / ``inflights`` / ``stats``.
    4. Periodic production is caller-owned: write your own
       ``@register_periodic_task`` that selects work items and calls
       ``task.submit(items)``.

    Common optional hooks:
    - ``work_item_id`` controls dedupe identity.
    - ``work_item_data`` controls context stored on the queued job.
    - ``skip_before_invoke`` re-checks stale state after dequeue.
    - ``on_execute_complete`` handles post-execute side effects.

    Item selection / producer-side pruning belongs before ``submit()``.
    ``namespace`` selects the queue and is the rate-limit isolation boundary.
    """

    task_key: str = ""
    namespace: str = ""
    config_cls: type[DispatchTaskConfig] = DispatchTaskConfig
    # Selects the Redis queue and dequeue policy for this task.
    # Subclasses should use their level-1 queue, e.g. AITaskQueue.
    queue_cls: type[DispatchQueue] = DispatchQueue

    def __init__(self, *, config: Optional[DispatchTaskConfig] = None):
        self.config = config or self.load_config()

    def load_config(self) -> DispatchTaskConfig:
        return self.config_cls.from_db()

    def build_queue_job(
        self,
        *,
        work_item_id: str,
        work_item_data: dict,
        payload_json: str = "",
        config_json: str = "",
        execute_at: Optional[float] = None,
    ) -> DispatchJob:
        return self.queue_cls.new_job(
            task_key=self.task_key,
            work_item_id=work_item_id,
            work_item_data=work_item_data,
            payload_json=payload_json,
            config_json=config_json,
            execute_at=execute_at,
        )

    def record_outcome(
        self,
        outcome: DispatchOutcomeType,
        *,
        elapsed_seconds: float = -1.0,
        sample_identity: str = "",
    ) -> None:
        self.queue_cls.record_outcome(
            self.task_key,
            outcome,
            elapsed_seconds=elapsed_seconds,
            sample_identity=sample_identity,
        )

    def requeue_job(self, job: DispatchJob, execute_at: float, *, job_ttl: int) -> bool:
        return QueueLifecycle.requeue(
            queue_cls=self.queue_cls,
            job=job,
            execute_at=execute_at,
            job_ttl=job_ttl,
        )

    @classmethod
    def register_queue_metadata(cls, metadata: dict) -> None:
        cls.queue_cls.register_task_metadata(cls.task_key, metadata)

    @classmethod
    def fetch_queued_job(cls, job_id: str) -> Optional[DispatchJob]:
        """Fetch a worker-side job payload through its namespace shard.

        ``job_id`` alone does not carry the namespace, so the primary path is
        in-process registry resolution. For unregistered task keys the payload
        key is namespace-bound and cannot be guessed: fan out over registered
        queue namespaces plus persisted route namespaces and stop at the first
        payload. No hit means the delivery is already missing.
        """
        namespace = namespace_for_job_id(job_id)
        if namespace:
            queue_cls = DispatchQueue.queue_for_namespace(namespace) or DispatchQueue.ephemeral_queue_for_namespace(
                namespace
            )
            return queue_cls.get_job(job_id)
        # fallback path
        for ns in sorted(candidate_namespaces()):
            queue_cls = DispatchQueue.queue_for_namespace(ns) or DispatchQueue.ephemeral_queue_for_namespace(ns)
            job = queue_cls.get_job(job_id)
            if job:
                return job
        return None

    @classmethod
    def discard_orphaned_job(cls, job_id: str) -> None:
        namespace = namespace_for_job_id(job_id)
        if namespace:
            queue_cls = DispatchQueue.queue_for_namespace(namespace) or DispatchQueue.ephemeral_queue_for_namespace(
                namespace
            )
            OrphanReaper.discard_orphaned_job(queue_cls, job_id)
            return
        OrphanReaper.discard_orphaned_job(DispatchQueue, job_id)

    def has_pending_work(self) -> bool:
        return self.queue_cls.has_pending_for_task(self.task_key)

    def has_inflight_work(self) -> bool:
        return self.queue_cls.has_inflight_for_task(self.task_key)

    def pending_work_count(self) -> int:
        return self.queue_cls.pending_count_for_task(self.task_key)

    def inflight_work_count(self) -> int:
        return self.queue_cls.inflight_count_for_task(self.task_key)

    def outcome_counts(self) -> dict[str, int]:
        return self.queue_cls.outcomes_for_task(self.task_key)

    @abstractmethod
    def execute(
        self,
        item: Any,
        *,
        job: Optional[DispatchJob] = None,
        overrides: Optional[dict] = None,
    ) -> DispatchOutcome:
        """Run one work item."""

    def on_execute_complete(
        self,
        item: Any,
        outcome: DispatchOutcome,
        *,
        job: Optional[DispatchJob] = None,
        overrides: Optional[dict] = None,
    ) -> None:
        """Optional post-execute hook."""

    def work_item_id(self, item: Any) -> str:
        if isinstance(item, dict):
            if "work_item_id" in item:
                return str(item["work_item_id"])
        return str(item)

    def work_item_data(self, item: Any) -> dict:
        if isinstance(item, dict):
            return dict(item)
        return {"value": item}

    def skip_before_invoke(self, item: Any) -> tuple[bool, str]:
        return False, ""

    def _config_for_job(self, overrides: Optional[dict] = None) -> DispatchTaskConfig:
        overrides = overrides or {}
        # Key presence matters: an explicit empty ``config={}`` is still an override.
        if "config" in overrides:
            return self.config_cls.from_raw({**self.config.to_raw(), **(overrides["config"] or {})})
        return self.config

    @abstractmethod
    def _build_job(
        self,
        item: Any,
        *,
        overrides: Optional[dict] = None,
        execute_at: Optional[float] = None,
        config: Optional[DispatchTaskConfig] = None,
    ) -> DispatchJob:
        """Build a queue job for one work item."""

    def _enqueue_outcome(
        self,
        job: DispatchJob,
        status: EnqueueStatus,
        *,
        record_unavailable: bool = True,
    ) -> DispatchOutcome:
        outcome = _ENQUEUE_OUTCOME_BY_STATUS.get(status, DispatchOutcomeType.ENQUEUE_UNAVAILABLE)
        metric_name = _ENQUEUE_METRIC_BY_OUTCOME[outcome]
        # Accepted/duplicate/capacity metrics are committed atomically by admission Lua.
        # Script failures cannot self-report, so record UNAVAILABLE here.
        if status == EnqueueStatus.UNAVAILABLE and record_unavailable:
            DispatchMetrics.record_enqueue_outcome(self.namespace, self.task_key, metric_name)
        logger.debug(
            "%s: work_item=%s outcome=%s",
            self.task_key,
            job.work_item_id,
            outcome,
        )
        return DispatchOutcome(outcome=outcome, extra={"work_item_id": job.work_item_id})

    @property
    def is_idle(self) -> bool:
        if not self.task_key:
            return False
        return not self.has_pending_work() and not self.has_inflight_work()

    @property
    def pendings(self) -> int:
        if not self.task_key:
            return -1
        return self.pending_work_count()

    @property
    def inflights(self) -> int:
        if not self.task_key:
            return -1
        return self.inflight_work_count()

    @property
    def stats(self) -> dict:
        if not self.task_key:
            return {"pending": -1, "inflight": -1, "outcomes": {}}
        return {
            "pending": self.pendings,
            "inflight": self.inflights,
            "outcomes": self.outcome_counts(),
        }

    def submit(self, items: Any, *, execute_at: ExecuteAtSpec = None, **overrides) -> list[DispatchOutcome]:
        """Submit work in bounded admission batches using one queue-config snapshot.

        ``execute_at`` schedules the pending score per item. It is a
        ``None`` / ``float`` / ``(index, item) -> float`` / schedule spec; see
        ``dispatch.scheduling`` (``at_front`` / ``spread``).
        ``index`` and schedule item counts span the whole ``submit`` call.
        """
        if isinstance(items, (list, tuple)):
            items = list(items)
        else:
            items = [items]

        if not items:
            return []
        execute_times = [
            resolve_execute_at(execute_at, index, item, count=len(items)) for index, item in enumerate(items)
        ]
        if any(value is not None and not math.isfinite(value) for value in execute_times):
            raise ValueError("execute_at must resolve to finite timestamps")
        task_config = self._config_for_job(overrides)
        queue_config = self.queue_cls.load_config()
        outcomes: list[DispatchOutcome] = []
        for offset in range(0, len(items), ADMISSION_BATCH_SIZE):
            chunk = items[offset : offset + ADMISSION_BATCH_SIZE]
            jobs = [
                self._build_job(
                    item,
                    overrides=overrides,
                    config=task_config,
                    execute_at=execute_times[offset + local_index],
                )
                for local_index, item in enumerate(chunk)
            ]
            ttl = task_config.resolve_queue_wait_ttl_seconds()
            statuses = QueueAdmission.enqueue_jobs(
                queue_cls=self.queue_cls,
                jobs=jobs,
                dedupe_enqueue=task_config.uses_queue_dedupe(),
                job_ttls=[ttl] * len(jobs),
                max_admitted_jobs=queue_config.max_admitted_jobs,
            )
            unavailable = sum(status == EnqueueStatus.UNAVAILABLE for status in statuses)
            if unavailable:
                DispatchMetrics.record_enqueue_outcome(
                    self.namespace,
                    self.task_key,
                    "enqueue_unavailable",
                    amount=unavailable,
                )
            outcomes.extend(
                self._enqueue_outcome(job, status, record_unavailable=False) for job, status in zip(jobs, statuses)
            )
        return outcomes

    def execute_from_job(self, job: DispatchJob) -> bool:
        """Execute one job; return whether it was successfully requeued."""
        execution_started_at = time.monotonic()
        item = job.work_item_data or {"work_item_id": job.work_item_id}
        skipped, reason = self.skip_before_invoke(item)
        if skipped:
            self.record_outcome(DispatchOutcomeType.SKIPPED)
            logger.debug(
                "%s: job=%s outcome=%s reason=%s",
                job.task_key,
                job.job_id,
                DispatchOutcomeType.SKIPPED,
                reason,
            )
            return False

        outcome = self.execute(item, job=job)
        elapsed_seconds = (
            outcome.elapsed_seconds
            if outcome.elapsed_seconds >= 0
            else max(0.0, time.monotonic() - execution_started_at)
        )
        self.record_outcome(
            outcome.outcome,
            elapsed_seconds=elapsed_seconds,
            sample_identity=job.sample_identity,
        )

        if outcome.should_requeue and job.retry_count < self.config.max_rate_limit_retries:
            job.retry_count += 1
            return self.requeue_job(
                job,
                time.time() + outcome.requeue_cooldown_seconds,
                job_ttl=self.config.resolve_queue_wait_ttl_seconds(),
            )

        self.on_execute_complete(item, outcome, job=job)
        return False

    @classmethod
    def execute_job(cls, job_id: str) -> None:
        # Deferred: registry imports DispatchTask from this module.
        from backend.db_periodic_task.dispatch.registry import DISPATCH_REGISTRY

        job = cls.fetch_queued_job(job_id)
        if not job:
            logger.warning("dispatch_execute: job not found job_id=%s", job_id)
            if namespace_for_job_id(job_id):
                # Registered task whose payload TTL expired: clean any leftovers.
                cls.discard_orphaned_job(job_id)
            else:
                # Unregistered task key with no payload anywhere: already
                # missing, nothing trustworthy to finalize or attribute.
                logger.warning("dispatch_execute: unregistered task_key with no payload job_id=%s", job_id)
            return

        task_cls = DISPATCH_REGISTRY.get(job.task_key)
        if not task_cls:
            logger.error("dispatch_execute: unknown task_key=%s job_id=%s", job.task_key, job_id)
            queue_cls = DispatchQueue.queue_for_namespace(
                job.namespace
            ) or DispatchQueue.ephemeral_queue_for_namespace(job.namespace)
            queue_cls.record_outcome(job.task_key, DispatchOutcomeType.ERROR)
            QueueLifecycle.finalize_job(
                queue_cls=queue_cls,
                job_id=job_id,
                task_key=job.task_key,
                work_item_id=job.work_item_id,
            )
            return

        execution_started_at = time.monotonic()
        requeued = False
        try:
            config = task_cls.queue_cls.config_for_job(job)
            if config is None:
                logger.error(
                    "dispatch_execute: config unavailable; drop task_key=%s job_id=%s",
                    job.task_key,
                    job_id,
                )
                task_cls.queue_cls.record_outcome(
                    job.task_key,
                    DispatchOutcomeType.ERROR,
                    elapsed_seconds=max(0.0, time.monotonic() - execution_started_at),
                    sample_identity=job.sample_identity,
                )
                return
            try:
                requeued = task_cls(config=config).execute_from_job(job)
            except Exception:
                task_cls.queue_cls.record_outcome(
                    job.task_key,
                    DispatchOutcomeType.ERROR,
                    elapsed_seconds=max(0.0, time.monotonic() - execution_started_at),
                    sample_identity=job.sample_identity,
                )
                raise
        finally:
            if not requeued:
                QueueLifecycle.finalize_job(
                    queue_cls=task_cls.queue_cls,
                    job_id=job_id,
                    task_key=job.task_key,
                    work_item_id=job.work_item_id,
                )

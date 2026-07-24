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

from backend.db_periodic_task.dispatch.outcomes import DispatchOutcomeType

logger = logging.getLogger("root")


def _is_worker_lost(exception) -> bool:
    try:
        from billiard.exceptions import WorkerLostError
    except ImportError:
        WorkerLostError = None
    return WorkerLostError is not None and isinstance(exception, WorkerLostError)


def _is_hard_time_limit_exceeded(exception) -> bool:
    """Whether the task was killed by Celery's hard ``--time-limit``.

    The soft limit is a subclass of ``TimeLimitExceeded`` but only warns: the
    task keeps running, so its inflight slot must NOT be dropped.
    """
    try:
        from celery.exceptions import SoftTimeLimitExceeded, TimeLimitExceeded
    except ImportError:
        return False
    return isinstance(exception, TimeLimitExceeded) and not isinstance(exception, SoftTimeLimitExceeded)


def dispatch_failure_handler(sender=None, task_id=None, exception=None, args=None, **_kwargs):
    """Log worker loss / unhandled errors; drop inflight on WorkerLost / hard time-limit kill.

    A hard time-limit kill is the threads-pool equivalent of ``WorkerLostError``
    (which is only raised by the prefork pool): the worker terminated the task
    mid-flight, so the job can never finalize itself and must be reclaimed
    immediately instead of occupying its inflight slot until the TTL reap.
    """
    is_worker_lost = _is_worker_lost(exception)
    is_time_limit = _is_hard_time_limit_exceeded(exception)
    task_name = getattr(sender, "name", "") or "<unknown>"
    logger.error(
        "%s: task_id=%s outcome=%s worker_lost=%s time_limit=%s exc_type=%s: %s",
        task_name,
        task_id,
        DispatchOutcomeType.ERROR,
        is_worker_lost,
        is_time_limit,
        type(exception).__name__ if exception else "None",
        exception,
    )
    if not (is_worker_lost or is_time_limit):
        return

    try:
        from backend.db_periodic_task.dispatch.lifecycle import QueueLifecycle
        from backend.db_periodic_task.dispatch.metrics import DispatchMetrics
        from backend.db_periodic_task.dispatch.queue import DispatchQueue

        job_id = str(args[0]) if args else ""
        if not job_id:
            return
        job = DispatchQueue.get_job(job_id)
        if not job:
            # Already finalized / TTL expired — nothing to drop.
            return

        DispatchMetrics.record_queue_counter(job.namespace, "celery_failure")
        DispatchMetrics.record_task_counter(job.task_key, "celery_failure")
        queue_cls = DispatchQueue.queue_for_namespace(job.namespace)
        if queue_cls is None:
            # Owning queue module is gone (removed app / failed discovery):
            # finalize through a throwaway queue bound to the job's namespace
            # so cleanup touches the keys where the job was actually reserved.
            queue_cls = DispatchQueue.ephemeral_queue_for_namespace(job.namespace)
        queue_cls.record_outcome(job.task_key, DispatchOutcomeType.ERROR)
        QueueLifecycle.finalize_job(
            queue_cls=queue_cls,
            job_id=job_id,
            task_key=job.task_key,
            work_item_id=job.work_item_id,
        )
        logger.warning(
            "dispatch: dropped inflight after worker_lost job_id=%s task_key=%s namespace=%s",
            job_id,
            job.task_key,
            job.namespace,
        )
    except Exception:
        logger.exception("dispatch: worker_lost drop failed task_id=%s", task_id)

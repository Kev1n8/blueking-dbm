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

import json
import logging
import time

from backend.db_periodic_task.dispatch.job import DispatchJob, compute_wait_deadline
from backend.db_periodic_task.dispatch.lua import register_script_once
from backend.db_periodic_task.dispatch.queue import TASK_MEMBERS_TTL_SECONDS

logger = logging.getLogger("root")

FINALIZE_JOB_LUA = """
local removed = redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('DEL', KEYS[1])
if KEYS[3] ~= '' then
    redis.call('DEL', KEYS[3])
end
if removed > 0 and ARGV[2] ~= '' then
    local value = tonumber(redis.call('HINCRBY', KEYS[4], ARGV[2], -1))
    if value <= 0 then
        redis.call('HDEL', KEYS[4], ARGV[2])
    end
    redis.call('EXPIRE', KEYS[4], ARGV[3])
end
return removed
"""

REQUEUE_JOB_LUA = """
local removed = redis.call('ZREM', KEYS[2], ARGV[1])
if removed == 0 then
    -- The job was already finalized or reaped while the worker was executing
    -- (e.g. execution outlived the inflight TTL). Never revive a zombie: no
    -- payload write, no pending ZADD, no counter move.
    return 0
end
redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
redis.call('ZADD', KEYS[3], ARGV[4], ARGV[1])
if removed > 0 and ARGV[5] ~= '' then
    local inflight_value = tonumber(redis.call('HINCRBY', KEYS[4], ARGV[5], -1))
    if inflight_value <= 0 then
        redis.call('HDEL', KEYS[4], ARGV[5])
    end
    local pending_value = tonumber(redis.call('HINCRBY', KEYS[4], ARGV[6], 1))
    if pending_value <= 0 then
        redis.call('HDEL', KEYS[4], ARGV[6])
    end
    -- ARGV[7] is the TTL of the rebuildable per-task count hash, not the job.
    redis.call('EXPIRE', KEYS[4], ARGV[7])
end
return removed
"""

_finalize_script = register_script_once(FINALIZE_JOB_LUA)
_requeue_script = register_script_once(REQUEUE_JOB_LUA)


class QueueLifecycle:
    """Terminal transitions (finalize / requeue) of one job on one queue.

    All operations bind to a queue class (``queue_cls``) and run as Lua scripts
    so job payload, zset membership, dedupe key, and the derived per-task count
    hash stay consistent under concurrency.
    """

    @classmethod
    def finalize_job(
        cls,
        *,
        queue_cls,
        job_id: str,
        task_key: str = "",
        work_item_id: str = "",
        task_members_ttl: int = TASK_MEMBERS_TTL_SECONDS,
    ) -> int:
        """Atomically flush terminal job-record, inflight, and dedupe cleanup."""
        try:
            dedupe_key = queue_cls.dedupe_key(task_key, work_item_id) if task_key and work_item_id else ""
            return int(
                _finalize_script(
                    keys=[
                        queue_cls._job_key(job_id),
                        queue_cls.inflight_key(),
                        dedupe_key,
                        queue_cls.task_members_key(),
                    ],
                    args=[
                        job_id,
                        queue_cls._inflight_member_field(task_key) if task_key else "",
                        max(1, int(task_members_ttl)),
                    ],
                )
                or 0
            )
        except Exception as exc:
            logger.warning("dispatch: finalize_job failed job_id=%s: %s", job_id, exc)
            return 0

    @classmethod
    def requeue_job(
        cls,
        *,
        queue_cls,
        job_id: str,
        job_snapshot: str,
        job_ttl: int,
        score: float,
        task_key: str = "",
        task_members_ttl: int = TASK_MEMBERS_TTL_SECONDS,
    ) -> int:
        """Requeue a job and refresh the rebuildable task-count hash TTL."""
        return int(
            _requeue_script(
                keys=[
                    queue_cls._job_key(job_id),
                    queue_cls.inflight_key(),
                    queue_cls.pending_key(),
                    queue_cls.task_members_key(),
                ],
                args=[
                    job_id,
                    job_snapshot,
                    max(1, int(job_ttl)),
                    float(score),
                    queue_cls._inflight_member_field(task_key) if task_key else "",
                    queue_cls._pending_member_field(task_key) if task_key else "",
                    max(1, int(task_members_ttl)),
                ],
            )
            or 0
        )

    @classmethod
    def requeue(cls, *, queue_cls, job: DispatchJob, execute_at: float, job_ttl: int) -> bool:
        """Requeue without extending an existing first-eligibility deadline.

        When the job's wait deadline has already expired, the job is discarded
        as orphaned instead. Returns whether the job was requeued.
        """
        try:
            now = time.time()
            deadline, ttl = compute_wait_deadline(now, execute_at, job_ttl, job.queue_deadline_at)
            if ttl is None:
                # Reservation clears the deadline (inflight is bounded by the
                # execution timeout instead); only an *existing* expired
                # deadline lands here and discards the job.
                from backend.db_periodic_task.dispatch.reaper import OrphanReaper

                OrphanReaper.discard_orphaned_job(
                    queue_cls,
                    job.job_id,
                    task_key=job.task_key,
                    namespace=job.namespace,
                )
                return False
            job.execute_at = execute_at
            job.queue_deadline_at = deadline
            requeued = cls.requeue_job(
                queue_cls=queue_cls,
                job_id=job.job_id,
                job_snapshot=json.dumps(job.to_dict(), ensure_ascii=False),
                job_ttl=ttl,
                score=execute_at,
                task_key=job.task_key,
            )
            if not requeued:
                # The job was already removed from inflight (finalized / reaped)
                # while the worker was executing; the caller must not treat this
                # as a successful requeue so its finalize path still runs.
                return False
            return True
        except Exception as exc:
            logger.warning("dispatch: requeue failed job_id=%s: %s", job.job_id, exc)
            return False

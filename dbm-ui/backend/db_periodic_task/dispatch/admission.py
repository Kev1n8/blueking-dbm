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
from enum import IntEnum

from backend.db_periodic_task.dispatch.job import DispatchJob, compute_wait_deadline
from backend.db_periodic_task.dispatch.lua import compile_script, eval_script
from backend.db_periodic_task.dispatch.metrics import DispatchMetrics
from backend.db_periodic_task.dispatch.queue import TASK_MEMBERS_CACHE_TTL_SECONDS

logger = logging.getLogger("root")

ADMISSION_BATCH_SIZE = 25

ENQUEUE_JOBS_LUA = """
local pending = KEYS[1]
local inflight = KEYS[2]
local task_metrics = KEYS[3]
local queue_metrics = KEYS[4]
local task_members = KEYS[5]
local producer_gate = KEYS[6]
local max_admitted = tonumber(ARGV[1])
local check_dedupe = tonumber(ARGV[2])
local task_field_prefix = ARGV[3]
local queue_field_prefix = ARGV[4]
local metrics_ttl = tonumber(ARGV[5])
local task_key = ARGV[6]
local task_members_ttl = tonumber(ARGV[7])
local job_count = tonumber(ARGV[8])

local admitted = redis.call('ZCARD', pending) + redis.call('ZCARD', inflight)
local accepted_count = 0
local metric_counts = {}
local statuses = {}
-- Producer gate: one EXISTS per admission batch, atomic with the writes below.
local producer_paused = (redis.call('EXISTS', producer_gate) == 1)

local function finish(index, status, metric_name)
    statuses[index] = status
    metric_counts[metric_name] = (metric_counts[metric_name] or 0) + 1
    return status
end

for index = 1, job_count do
    local key_offset = 6 + ((index - 1) * 2)
    local arg_offset = 8 + ((index - 1) * 4)
    local job_record = KEYS[key_offset + 1]
    local dedupe_key = KEYS[key_offset + 2]
    local job_id = ARGV[arg_offset + 1]
    local score = tonumber(ARGV[arg_offset + 2])
    local job_snapshot = ARGV[arg_offset + 3]
    local record_ttl = tonumber(ARGV[arg_offset + 4])
    local decided = false

    -- Closed producer gate rejects everything before dedupe / capacity: a
    -- paused producer must never consume queue slots or dedupe identities.
    if producer_paused then
        finish(index, -4, 'enqueue_producer_rejected')
        decided = true
    end

    if not decided and check_dedupe == 1 then
        if redis.call('ZSCORE', pending, job_id) ~= false then
            finish(index, -1, 'enqueue_duplicate')
            decided = true
        elseif redis.call('ZSCORE', inflight, job_id) ~= false then
            finish(index, -1, 'enqueue_duplicate')
            decided = true
        elseif dedupe_key ~= '' and redis.call('EXISTS', dedupe_key) == 1 then
            finish(index, -1, 'enqueue_duplicate')
            decided = true
        end
    end

    if not decided and admitted >= max_admitted then
        finish(index, 0, 'enqueue_capacity_rejected')
        decided = true
    end

    if not decided and check_dedupe == 1 and dedupe_key ~= '' then
        local ok = redis.call('SET', dedupe_key, job_id, 'NX', 'EX', record_ttl)
        if not ok then
            finish(index, -1, 'enqueue_duplicate')
            decided = true
        end
    end

    if not decided then
        redis.call('SET', job_record, job_snapshot, 'EX', record_ttl)
        redis.call('ZADD', pending, score, job_id)
        admitted = admitted + 1
        accepted_count = accepted_count + 1
        finish(index, 1, 'enqueued')
    end
end

if task_key ~= '' and accepted_count > 0 then
    local pending_value = tonumber(redis.call('HINCRBY', task_members, 'pending:' .. task_key, accepted_count))
    if pending_value <= 0 then
        redis.call('HDEL', task_members, 'pending:' .. task_key)
    end
    redis.call('EXPIRE', task_members, task_members_ttl)
end

local has_metrics = false
for metric_name, amount in pairs(metric_counts) do
    has_metrics = true
    -- Metrics remain fail-open: telemetry corruption must not reject admission.
    redis.pcall('HINCRBY', task_metrics, task_field_prefix .. metric_name, amount)
    redis.pcall('HINCRBY', queue_metrics, queue_field_prefix .. metric_name, amount)
end
if has_metrics then
    redis.pcall('EXPIRE', task_metrics, metrics_ttl)
    redis.pcall('EXPIRE', queue_metrics, metrics_ttl)
end
return statuses
"""


class EnqueueStatus(IntEnum):
    UNAVAILABLE = -2
    DUPLICATE = -1
    DEADLINE_EXPIRED = -3
    PRODUCER_REJECTED = -4
    CAPACITY_REJECTED = 0
    ACCEPTED = 1


class QueueAdmission:
    """Atomically admit jobs under admitted capacity and optional dedupe."""

    _enqueue_script = compile_script(ENQUEUE_JOBS_LUA)

    @classmethod
    def enqueue_jobs(
        cls,
        *,
        queue_cls,
        jobs: list[DispatchJob],
        dedupe_enqueue: bool,
        job_ttls: list[int],
        max_admitted_jobs: int,
    ) -> list[EnqueueStatus]:
        if not jobs:
            return []
        if len(jobs) != len(job_ttls):
            raise ValueError("jobs and job_ttls must have the same length")
        if len(jobs) > ADMISSION_BATCH_SIZE:
            raise ValueError(f"admission batch cannot exceed {ADMISSION_BATCH_SIZE} jobs")
        task_keys = {job.task_key for job in jobs}
        if len(task_keys) != 1:
            raise ValueError("all jobs in an admission batch must share one task_key")

        statuses: list[EnqueueStatus | None] = [None] * len(jobs)
        prepared: list[tuple[int, DispatchJob, int]] = []
        now = time.time()
        for index, (job, job_ttl) in enumerate(zip(jobs, job_ttls)):
            deadline, ttl = compute_wait_deadline(now, job.execute_at, job_ttl, job.queue_deadline_at)
            if ttl is None:
                # Existing deadline already expired: nothing left of the wait
                # budget. Distinct from CAPACITY_REJECTED so producers holding
                # cursors on admission failure do not misread an expired job
                # (which must never be retried) as a full queue.
                statuses[index] = EnqueueStatus.DEADLINE_EXPIRED
                continue
            job.queue_deadline_at = deadline
            prepared.append((index, job, ttl))

        if prepared:
            task_key = jobs[0].task_key
            task_metrics, queue_metrics, task_prefix, queue_prefix, metrics_ttl = DispatchMetrics.enqueue_counter_spec(
                queue_cls._ns(), task_key
            )
            keys = [
                queue_cls.pending_key(),
                queue_cls.inflight_key(),
                task_metrics,
                queue_metrics,
                queue_cls.task_members_key(),
                queue_cls.producer_lock_key(),
            ]
            args = [
                max(1, int(max_admitted_jobs)),
                1 if dedupe_enqueue else 0,
                task_prefix,
                queue_prefix,
                metrics_ttl,
                task_key or "",
                TASK_MEMBERS_CACHE_TTL_SECONDS,
                len(prepared),
            ]
            for _index, job, ttl in prepared:
                dedupe_key = (
                    queue_cls.dedupe_key(job.task_key, job.work_item_id)
                    if dedupe_enqueue and job.task_key and job.work_item_id
                    else ""
                )
                keys.extend([queue_cls._job_key(job.job_id), dedupe_key])
                args.extend(
                    [
                        job.job_id,
                        float(job.execute_at),
                        json.dumps(job.to_dict(), ensure_ascii=False),
                        ttl,
                    ]
                )
            try:
                results = eval_script(cls._enqueue_script, client=queue_cls.conn(), keys=keys, args=args)
                if len(prepared) == 1 and not isinstance(results, (list, tuple)):
                    results = [results]
                for (index, _job, _ttl), result in zip(prepared, results):
                    statuses[index] = EnqueueStatus(int(result))
            except Exception:
                for index, _job, _ttl in prepared:
                    statuses[index] = EnqueueStatus.UNAVAILABLE

        resolved = [EnqueueStatus.UNAVAILABLE if status is None else status for status in statuses]
        unavailable = sum(status == EnqueueStatus.UNAVAILABLE for status in resolved)
        if unavailable:
            logger.warning(
                "dispatch: batch enqueue unavailable namespace=%s unavailable=%d total=%d",
                queue_cls._ns(),
                unavailable,
                len(jobs),
            )
        return resolved

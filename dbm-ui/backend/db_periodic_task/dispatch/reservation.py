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
from dataclasses import replace
from enum import IntEnum

from backend.db_periodic_task.dispatch.config import PUMP_INTERVAL_SECONDS, DispatchQueueConfig
from backend.db_periodic_task.dispatch.job import DispatchJob
from backend.db_periodic_task.dispatch.lua import compile_script, eval_script
from backend.db_periodic_task.dispatch.queue import TASK_MEMBERS_CACHE_TTL_SECONDS, DispatchQueue

logger = logging.getLogger("root")

# Keep each EVAL short so other Redis clients are not blocked for long.
RESERVATION_PIPELINE_CHUNK_SIZE = 25
TICK_COUNTER_TTL_SECONDS = PUMP_INTERVAL_SECONDS * 6

RESERVE_JOB_LUA = """
local pending = KEYS[1]
local inflight = KEYS[2]
local tick_counter = KEYS[3]
local task_members = KEYS[4]
local now = tonumber(ARGV[1])
local max_inflight = tonumber(ARGV[2])
local tick_budget = tonumber(ARGV[3])
local counter_ttl = tonumber(ARGV[4])
local job_count = tonumber(ARGV[5])
local task_members_ttl = tonumber(ARGV[6])

local inflight_count = tonumber(redis.call('ZCARD', inflight))
local used = tonumber(redis.call('GET', tick_counter) or '0')
local reserved_count = 0
local statuses = {}

for index = 1, job_count do
    local key_offset = 4 + ((index - 1) * 2)
    local arg_offset = 6 + ((index - 1) * 4)
    local job_record = KEYS[key_offset + 1]
    local dedupe_key = KEYS[key_offset + 2]
    local job_id = ARGV[arg_offset + 1]
    local job_snapshot = ARGV[arg_offset + 2]
    local record_ttl = tonumber(ARGV[arg_offset + 3])
    local task_key = ARGV[arg_offset + 4]

    if redis.call('ZSCORE', pending, job_id) == false then
        statuses[index] = -1
    elseif inflight_count >= max_inflight or used >= tick_budget then
        statuses[index] = 0
    else
        redis.call('ZREM', pending, job_id)
        redis.call('ZADD', inflight, now, job_id)
        redis.call('SET', job_record, job_snapshot, 'EX', record_ttl)
        if dedupe_key ~= '' then
            redis.call('EXPIRE', dedupe_key, record_ttl)
        end
        if task_key ~= '' then
            local pending_field = 'pending:' .. task_key
            local pending_value = tonumber(redis.call('HINCRBY', task_members, pending_field, -1))
            if pending_value <= 0 then
                redis.call('HDEL', task_members, pending_field)
            end
            local inflight_field = 'inflight:' .. task_key
            local inflight_value = tonumber(redis.call('HINCRBY', task_members, inflight_field, 1))
            if inflight_value <= 0 then
                redis.call('HDEL', task_members, inflight_field)
            end
            redis.call('EXPIRE', task_members, task_members_ttl)
        end
        inflight_count = inflight_count + 1
        used = used + 1
        reserved_count = reserved_count + 1
        statuses[index] = 1
    end
end

if reserved_count > 0 then
    redis.call('SET', tick_counter, used, 'EX', counter_ttl)
end
return statuses
"""


class ReservationStatus(IntEnum):
    MISSING = -1
    BLOCKED = 0
    RESERVED = 1


class QueueReservation:
    """Atomically reserve FIFO jobs under queue inflight and tick ceilings."""

    _reserve_script = compile_script(RESERVE_JOB_LUA)

    @classmethod
    def reserve_jobs(
        cls,
        jobs: list[DispatchJob],
        config: DispatchQueueConfig,
        *,
        queue_cls: type[DispatchQueue],
        record_ttls: list[int],
        tick_id: int,
        tick_budget: int,
    ) -> list[ReservationStatus]:
        if not jobs:
            return []
        if len(jobs) != len(record_ttls):
            raise ValueError("jobs and record_ttls must have the same length")
        if len(jobs) > RESERVATION_PIPELINE_CHUNK_SIZE:
            raise ValueError(f"reservation chunk cannot exceed {RESERVATION_PIPELINE_CHUNK_SIZE} jobs")

        keys = [
            queue_cls.pending_key(),
            queue_cls.inflight_key(),
            queue_cls.tick_counter_key(tick_id),
            queue_cls.task_members_key(),
        ]
        args = [
            time.time(),
            max(1, int(config.max_inflight)),
            max(1, int(tick_budget)),
            TICK_COUNTER_TTL_SECONDS,
            len(jobs),
            TASK_MEMBERS_CACHE_TTL_SECONDS,
        ]
        for job, record_ttl in zip(jobs, record_ttls):
            reserved_job = replace(job, queue_deadline_at=0.0)
            keys.extend(
                [
                    queue_cls._job_key(job.job_id),
                    queue_cls.dedupe_key(job.task_key, job.work_item_id) if job.task_key and job.work_item_id else "",
                ]
            )
            args.extend(
                [
                    job.job_id,
                    json.dumps(reserved_job.to_dict(), ensure_ascii=False),
                    max(1, int(record_ttl)),
                    job.task_key or "",
                ]
            )
        try:
            results = eval_script(cls._reserve_script, client=queue_cls.conn(), keys=keys, args=args)
        except Exception as exc:
            logger.warning(
                "dispatch: reserve_jobs failed namespace=%s count=%d: %s",
                queue_cls._ns(),
                len(jobs),
                exc,
            )
            return [ReservationStatus.BLOCKED] * len(jobs)
        return [ReservationStatus(int(result)) for result in results]

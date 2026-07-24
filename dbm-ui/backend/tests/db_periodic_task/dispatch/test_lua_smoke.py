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
# Smoke tests that execute the dispatch Lua scripts against the real Redis
# configured for the test environment (same convention as other suites that
# use ``django.core.cache`` / ``RedisConn`` unpatched).
#
# The mock-based unit tests assert call shapes; these verify actual script
# behavior: enqueue capacity/dedupe, reserve/finalize/requeue membership and
# counter moves, atomic purge decrements (A1), and namespace-scoped dedupe
# cleanup on orphan discard (A2).
#
# All keys live under smoke-specific namespaces and are deleted before and
# after each test; nothing touches production-looking dispatch keys.
import json
import time

import pytest

from backend.db_periodic_task.dispatch import lifecycle
from backend.db_periodic_task.dispatch.admission import EnqueueStatus, QueueAdmission
from backend.db_periodic_task.dispatch.config import DispatchQueueConfig
from backend.db_periodic_task.dispatch.job import DispatchJob, build_job_id
from backend.db_periodic_task.dispatch.queue import DISPATCH_QUEUE_REGISTRY, TASK_MEMBERS_TTL_SECONDS, DispatchQueue
from backend.db_periodic_task.dispatch.reaper import OrphanReaper
from backend.db_periodic_task.dispatch.reservation import QueueReservation, ReservationStatus
from backend.utils.redis import RedisConn

TASK_KEY = "smoke.task"
NAMESPACE = "smoke"
NAMESPACE_TWO = "smoke2"

# Key patterns this suite may create; cleaned before/after every test.
_KEY_PATTERNS = [
    f"dispatch:{NAMESPACE}:*",
    f"dispatch:{NAMESPACE_TWO}:*",
    f"dispatch:job:{TASK_KEY}:*",
    f"dispatch:dedupe:{NAMESPACE}:*",
    f"dispatch:dedupe:{NAMESPACE_TWO}:*",
    f"dispatch:metrics:queue:{NAMESPACE}:*",
    f"dispatch:metrics:queue:{NAMESPACE_TWO}:*",
    f"dispatch:metrics:task:{TASK_KEY}:*",
    f"dispatch:metrics:sample:{NAMESPACE}:*",
    f"dispatch:metrics:sample:{NAMESPACE_TWO}:*",
]


def _delete_test_keys() -> None:
    for pattern in _KEY_PATTERNS:
        keys = list(RedisConn.scan_iter(match=pattern, count=500))
        if keys:
            RedisConn.delete(*keys)


def _make_queue_class(namespace: str) -> type[DispatchQueue]:
    config_cls = type(f"SmokeQueueConfig:{namespace}", (DispatchQueueConfig,), {"namespace": namespace})
    return type(f"SmokeQueue:{namespace}", (DispatchQueue,), {"config_cls": config_cls})


def _job(work_item_id: str, *, execute_at=None) -> DispatchJob:
    return DispatchJob(
        job_id=build_job_id(TASK_KEY, work_item_id),
        task_key=TASK_KEY,
        namespace=NAMESPACE,
        work_item_id=work_item_id,
        created_at=time.time(),
        execute_at=execute_at or time.time(),
    )


def _enqueue(queue_cls, jobs, *, max_admitted_jobs=10):
    return QueueAdmission.enqueue_jobs(
        queue_cls=queue_cls,
        jobs=jobs,
        dedupe_enqueue=True,
        job_ttls=[300] * len(jobs),
        max_admitted_jobs=max_admitted_jobs,
    )


def _reserve(queue_cls, jobs, *, max_inflight=2, tick_budget=10):
    return QueueReservation.reserve_jobs(
        jobs,
        DispatchQueueConfig(max_inflight=max_inflight),
        queue_cls=queue_cls,
        record_ttls=[300] * len(jobs),
        tick_id=100,
        tick_budget=tick_budget,
    )


@pytest.fixture
def live_redis():
    """Real Redis with smoke-namespace cleanup and queue-registry isolation."""
    from unittest.mock import patch

    saved_registry = dict(DISPATCH_QUEUE_REGISTRY)
    DISPATCH_QUEUE_REGISTRY.clear()
    _delete_test_keys()
    with patch.object(DispatchQueue, "ensure_queues_loaded"):
        yield RedisConn
    DISPATCH_QUEUE_REGISTRY.clear()
    DISPATCH_QUEUE_REGISTRY.update(saved_registry)
    _delete_test_keys()


class TestEnqueueLua:
    def test_same_work_item_second_enqueue_is_duplicate(self, live_redis):
        queue_cls = _make_queue_class(NAMESPACE)

        assert _enqueue(queue_cls, [_job("item-1")]) == [EnqueueStatus.ACCEPTED]
        assert _enqueue(queue_cls, [_job("item-1")]) == [EnqueueStatus.DUPLICATE]

        assert live_redis.zcard(queue_cls.pending_key()) == 1
        assert live_redis.exists(queue_cls.dedupe_key(TASK_KEY, "item-1")) == 1
        assert live_redis.hget(queue_cls.task_members_key(), f"pending:{TASK_KEY}") == "1"

    def test_capacity_rejects_beyond_max_admitted(self, live_redis):
        queue_cls = _make_queue_class(NAMESPACE)

        statuses = _enqueue(queue_cls, [_job("a"), _job("b"), _job("c")], max_admitted_jobs=2)

        assert statuses == [EnqueueStatus.ACCEPTED, EnqueueStatus.ACCEPTED, EnqueueStatus.CAPACITY_REJECTED]
        assert live_redis.zcard(queue_cls.pending_key()) == 2
        assert live_redis.hget(queue_cls.task_members_key(), f"pending:{TASK_KEY}") == "2"


class TestReserveLua:
    def test_reserve_moves_pending_to_inflight_with_counts(self, live_redis):
        queue_cls = _make_queue_class(NAMESPACE)
        jobs = [_job("a"), _job("b"), _job("c")]
        _enqueue(queue_cls, jobs)

        statuses = _reserve(queue_cls, jobs, max_inflight=2)

        assert statuses == [ReservationStatus.RESERVED, ReservationStatus.RESERVED, ReservationStatus.BLOCKED]
        assert live_redis.zcard(queue_cls.pending_key()) == 1
        assert live_redis.zcard(queue_cls.inflight_key()) == 2
        assert live_redis.hgetall(queue_cls.task_members_key()) == {
            f"pending:{TASK_KEY}": "1",
            f"inflight:{TASK_KEY}": "2",
        }

    def test_reserve_missing_job_is_noop(self, live_redis):
        queue_cls = _make_queue_class(NAMESPACE)

        assert _reserve(queue_cls, [_job("ghost")]) == [ReservationStatus.MISSING]
        assert live_redis.zcard(queue_cls.inflight_key()) == 0
        assert live_redis.hgetall(queue_cls.task_members_key()) == {}


class TestFinalizeLua:
    def test_finalize_clears_inflight_payload_dedupe_and_count(self, live_redis):
        queue_cls = _make_queue_class(NAMESPACE)
        job = _job("a")
        _enqueue(queue_cls, [job])
        _reserve(queue_cls, [job])

        removed = lifecycle.QueueLifecycle.finalize_job(
            queue_cls=queue_cls,
            job_id=job.job_id,
            task_key=TASK_KEY,
            work_item_id="a",
            task_members_ttl=TASK_MEMBERS_TTL_SECONDS,
        )

        assert removed == 1
        assert live_redis.zcard(queue_cls.inflight_key()) == 0
        assert live_redis.exists(queue_cls._job_key(job.job_id)) == 0
        assert live_redis.exists(queue_cls.dedupe_key(TASK_KEY, "a")) == 0
        assert live_redis.hgetall(queue_cls.task_members_key()) == {}


class TestRequeueLua:
    def test_requeue_moves_inflight_back_to_pending(self, live_redis):
        queue_cls = _make_queue_class(NAMESPACE)
        job = _job("a")
        _enqueue(queue_cls, [job])
        _reserve(queue_cls, [job])

        removed = lifecycle.QueueLifecycle.requeue_job(
            queue_cls=queue_cls,
            job_id=job.job_id,
            job_snapshot=json.dumps(job.to_dict(), ensure_ascii=False),
            job_ttl=300,
            score=time.time(),
            task_key=TASK_KEY,
            task_members_ttl=TASK_MEMBERS_TTL_SECONDS,
        )

        assert removed == 1
        assert live_redis.zcard(queue_cls.pending_key()) == 1
        assert live_redis.zcard(queue_cls.inflight_key()) == 0
        assert live_redis.hgetall(queue_cls.task_members_key()) == {f"pending:{TASK_KEY}": "1"}

    def test_requeue_after_finalize_does_not_revive(self, live_redis):
        """P2-9: requeue must never resurrect a job the reap/finalize already cleaned."""
        queue_cls = _make_queue_class(NAMESPACE)
        job = _job("a")
        _enqueue(queue_cls, [job])
        _reserve(queue_cls, [job])
        # The job is cleaned up while the worker is still executing (finalize or
        # orphan reap): inflight member, payload and dedupe are all gone.
        lifecycle.QueueLifecycle.finalize_job(
            queue_cls=queue_cls,
            job_id=job.job_id,
            task_key=TASK_KEY,
            work_item_id="a",
            task_members_ttl=TASK_MEMBERS_TTL_SECONDS,
        )

        removed = lifecycle.QueueLifecycle.requeue_job(
            queue_cls=queue_cls,
            job_id=job.job_id,
            job_snapshot=json.dumps(job.to_dict(), ensure_ascii=False),
            job_ttl=300,
            score=time.time(),
            task_key=TASK_KEY,
            task_members_ttl=TASK_MEMBERS_TTL_SECONDS,
        )

        # No zombie: no payload write, no pending ZADD, no counter move.
        assert removed == 0
        assert live_redis.zcard(queue_cls.pending_key()) == 0
        assert live_redis.zcard(queue_cls.inflight_key()) == 0
        assert live_redis.exists(queue_cls._job_key(job.job_id)) == 0
        assert live_redis.hgetall(queue_cls.task_members_key()) == {}


class TestPurgeMemberLua:
    def test_repeated_purge_does_not_double_decrement(self, live_redis):
        """A1: reap must decrement only what it actually removed, atomically."""
        queue_cls = _make_queue_class(NAMESPACE)
        jobs = [_job("a"), _job("b")]
        _enqueue(queue_cls, jobs)

        assert OrphanReaper.purge_member(queue_cls, jobs[0].job_id, task_key=TASK_KEY) == (1, 0)
        assert live_redis.hget(queue_cls.task_members_key(), f"pending:{TASK_KEY}") == "1"

        # A second reap of the same member (concurrent ZSCAN race) is a no-op.
        assert OrphanReaper.purge_member(queue_cls, jobs[0].job_id, task_key=TASK_KEY) == (0, 0)
        assert live_redis.hget(queue_cls.task_members_key(), f"pending:{TASK_KEY}") == "1"

    def test_purge_inflight_member_after_reserve(self, live_redis):
        queue_cls = _make_queue_class(NAMESPACE)
        job = _job("a")
        _enqueue(queue_cls, [job])
        _reserve(queue_cls, [job])

        assert OrphanReaper.purge_member(queue_cls, job.job_id, task_key=TASK_KEY) == (0, 1)
        assert live_redis.zcard(queue_cls.inflight_key()) == 0
        assert live_redis.hgetall(queue_cls.task_members_key()) == {}

    def test_purge_without_task_key_touches_no_counters(self, live_redis):
        queue_cls = _make_queue_class(NAMESPACE)
        job = _job("a")
        _enqueue(queue_cls, [job])

        assert OrphanReaper.purge_member(queue_cls, job.job_id) == (1, 0)
        # Unknown task attribution: counters stay for the drift rebuild to own.
        assert live_redis.hget(queue_cls.task_members_key(), f"pending:{TASK_KEY}") == "1"


class TestDiscardOrphanedJobScoping:
    def test_unknown_namespace_deletes_dedupe_only_where_member_lived(self, live_redis):
        """A2: orphan discard must not break another queue's live dedupe key."""
        queue_cls = _make_queue_class(NAMESPACE)
        other_queue_cls = _make_queue_class(NAMESPACE_TWO)
        job = _job("item-1")
        _enqueue(queue_cls, [job])
        # Another queue happens to use the same task_key + work_item_id pair.
        live_redis.set(other_queue_cls.dedupe_key(TASK_KEY, "item-1"), "other-job", ex=300)
        # Payload TTL expires -> the job becomes an orphan.
        live_redis.delete(queue_cls._job_key(job.job_id))

        # Invoke via the owning queue class so outcome metrics stay smoke-scoped.
        assert OrphanReaper.discard_orphaned_job(queue_cls, job.job_id, task_key=TASK_KEY, work_item_id="item-1")

        assert live_redis.zcard(queue_cls.pending_key()) == 0
        assert live_redis.exists(queue_cls.dedupe_key(TASK_KEY, "item-1")) == 0
        assert live_redis.get(other_queue_cls.dedupe_key(TASK_KEY, "item-1")) == "other-job"

    def test_unknown_namespace_without_member_keeps_dedupe_for_ttl(self, live_redis):
        """A2: when no queue claims the member, dedupe keys are left to TTL."""
        queue_cls = _make_queue_class(NAMESPACE)
        live_redis.set(queue_cls.dedupe_key(TASK_KEY, "item-1"), "live-job", ex=300)

        assert OrphanReaper.discard_orphaned_job(
            queue_cls,
            build_job_id(TASK_KEY, "item-1"),
            task_key=TASK_KEY,
            work_item_id="item-1",
        )

        assert live_redis.get(queue_cls.dedupe_key(TASK_KEY, "item-1")) == "live-job"

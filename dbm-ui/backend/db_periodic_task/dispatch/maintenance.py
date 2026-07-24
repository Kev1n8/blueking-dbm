# -*- coding: utf-8 -*-
"""Out-of-band maintenance for derived dispatch queue membership counters."""

import logging
import time
import uuid
from datetime import timedelta
from typing import Optional

from backend.db_periodic_task.dispatch.config import (
    TASK_MEMBERS_REBUILD_DEADLINE_SECONDS,
    TASK_MEMBERS_REBUILD_LOCK_SECONDS,
    TASK_MEMBERS_REBUILD_PERIOD_SECONDS,
)
from backend.db_periodic_task.dispatch.lua import register_script_once
from backend.db_periodic_task.dispatch.queue import DispatchQueue
from backend.db_periodic_task.dispatch.task_members import TaskMembers
from backend.db_periodic_task.register import register_periodic_task
from backend.utils.redis import RedisConn

logger = logging.getLogger("root")

TASK_MEMBERS_REBUILD_LOCK_PREFIX = "dispatch:task_members_rebuild_lock:"
RELEASE_REBUILD_LOCK_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""
_release_rebuild_lock_script = register_script_once(RELEASE_REBUILD_LOCK_LUA)


def _rebuild_lock_key(namespace: str) -> str:
    return f"{TASK_MEMBERS_REBUILD_LOCK_PREFIX}{namespace}"


def _try_acquire_rebuild_lock(namespace: str, owner: str) -> bool:
    try:
        return bool(
            RedisConn.set(
                _rebuild_lock_key(namespace),
                owner,
                nx=True,
                ex=TASK_MEMBERS_REBUILD_LOCK_SECONDS,
            )
        )
    except Exception as exc:
        logger.warning("dispatch maintenance: lock failed namespace=%s: %s", namespace, exc)
        return False


def _release_rebuild_lock(namespace: str, owner: str) -> None:
    try:
        _release_rebuild_lock_script(keys=[_rebuild_lock_key(namespace)], args=[owner])
    except Exception as exc:
        logger.warning("dispatch maintenance: lock release failed namespace=%s: %s", namespace, exc)


def _maintain_queue(queue_cls: type[DispatchQueue], trigger: str) -> Optional[bool]:
    """Attempt one namespace; return None when lock/backoff skipped the scan."""
    namespace = queue_cls.namespace
    owner = f"task-members:{namespace}:{uuid.uuid4().hex}"
    if not _try_acquire_rebuild_lock(namespace, owner):
        return None
    started_at = time.monotonic()
    try:
        if not TaskMembers.try_start_rebuild(queue_cls):
            return None
        rebuilt = TaskMembers.rebuild(
            queue_cls,
            deadline_at=started_at + TASK_MEMBERS_REBUILD_DEADLINE_SECONDS,
        )
        elapsed = time.monotonic() - started_at
        if rebuilt is None:
            logger.warning(
                "dispatch maintenance: rebuild aborted namespace=%s trigger=%s duration=%.3fs",
                namespace,
                trigger,
                elapsed,
            )
            return False
        if not TaskMembers.mark_rebuilt(queue_cls):
            logger.warning(
                "dispatch maintenance: success marker failed namespace=%s trigger=%s",
                namespace,
                trigger,
            )
            return False
        logger.info(
            "dispatch maintenance: rebuild complete namespace=%s trigger=%s duration=%.3fs fields=%d",
            namespace,
            trigger,
            elapsed,
            len(rebuilt),
        )
        return True
    except Exception as exc:
        logger.warning(
            "dispatch maintenance: rebuild failed namespace=%s trigger=%s: %s",
            namespace,
            trigger,
            exc,
        )
        return False
    finally:
        _release_rebuild_lock(namespace, owner)


@register_periodic_task(run_every=timedelta(seconds=TASK_MEMBERS_REBUILD_PERIOD_SECONDS))
def dispatch_task_members_maintenance():
    """Attempt at most one requested or daily namespace rebuild per run."""
    queues = DispatchQueue.iter_queues()
    requested = [queue_cls for queue_cls in queues if TaskMembers.rebuild_requested(queue_cls)]
    requested_set = set(requested)
    daily = [
        queue_cls for queue_cls in queues if queue_cls not in requested_set and TaskMembers.hard_rebuild_due(queue_cls)
    ]
    for trigger, candidates in (("requested", requested), ("daily", daily)):
        for queue_cls in candidates:
            result = _maintain_queue(queue_cls, trigger)
            if result is not None:
                return {
                    "attempted": 1,
                    "namespace": queue_cls.namespace,
                    "trigger": trigger,
                    "success": result,
                }
    return {"attempted": 0}

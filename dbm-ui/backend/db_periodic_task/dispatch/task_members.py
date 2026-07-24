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
import time
from collections import defaultdict
from typing import Optional

from backend.db_periodic_task.dispatch.config import (
    TASK_MEMBERS_REBUILD_FORCE_SECONDS,
    TASK_MEMBERS_REBUILD_REQUEST_TTL_SECONDS,
    TASK_MEMBERS_REBUILD_RETRY_SECONDS,
    TASK_MEMBERS_REBUILD_SCAN_COUNT,
    TASK_MEMBERS_REBUILD_SCAN_PAUSE_SECONDS,
)
from backend.db_periodic_task.dispatch.job import resolve_task_key_from_job_id
from backend.db_periodic_task.dispatch.lua import register_script_once
from backend.db_periodic_task.dispatch.metrics import _text
from backend.db_periodic_task.dispatch.queue import (
    KEY_REGISTERED,
    TASK_MEMBERS_TTL_SECONDS,
    DispatchQueue,
    set_redis_ttl_marker,
)
from backend.utils.redis import RedisConn

logger = logging.getLogger("root")

# Pseudo-task key collecting members whose job_id prefix matches no registered
# task (e.g. jobs left behind by an unregistered task). Bucketing them keeps
# the rebuild completing instead of aborting the whole namespace, and the field
# doubles as an alerting signal for stale membership.
UNRESOLVED_TASK_KEY = "__unresolved__"

# Replace the derived per-task hash only if queue totals still match the scan.
# Admission/reservation/finalize scripts cannot interleave with this Lua check.
REPLACE_TASK_MEMBERS_LUA = """
local pending = tonumber(redis.call('ZCARD', KEYS[1]))
local inflight = tonumber(redis.call('ZCARD', KEYS[2]))
local expected_pending = tonumber(ARGV[1])
local expected_inflight = tonumber(ARGV[2])
if pending ~= expected_pending or inflight ~= expected_inflight then
    return 0
end

local field_count = tonumber(ARGV[3])
redis.call('DEL', KEYS[3])
for index = 1, field_count do
    local offset = 5 + ((index - 1) * 2)
    redis.call('HSET', KEYS[3], ARGV[offset], ARGV[offset + 1])
end
if field_count > 0 then
    redis.call('EXPIRE', KEYS[3], ARGV[4])
end
return 1
"""
_replace_task_members_script = register_script_once(REPLACE_TASK_MEMBERS_LUA)


def _parse_task_member_field(field: str) -> Optional[tuple[str, str]]:
    """Return ``(kind, task_key)`` for a ``task_members`` hash field, else ``None``."""
    if field.startswith("pending:"):
        return "pending", field[len("pending:") :]
    if field.startswith("inflight:"):
        return "inflight", field[len("inflight:") :]
    return None


def _summarize_task_members_hash(raw: dict) -> Optional[tuple[int, int]]:
    """Parse ``task_members`` hash into pending/inflight sums.

    Returns ``None`` when a field value is invalid (treated as drift evidence).
    """
    pending_sum = 0
    inflight_sum = 0

    for raw_field, raw_value in raw.items():
        parsed = _parse_task_member_field(_text(raw_field))
        if parsed is None:
            continue
        kind, _task_key = parsed
        try:
            value = int(raw_value)
        except (TypeError, ValueError):
            return None
        if value < 0:
            return None
        if kind == "pending":
            pending_sum += value
        else:
            inflight_sum += value
    return pending_sum, inflight_sum


def _task_member_mapping(
    pending_counts: dict[str, int],
    inflight_counts: dict[str, int],
) -> dict[str, int]:
    mapping = {
        DispatchQueue._pending_member_field(task_key): count for task_key, count in pending_counts.items() if count > 0
    }
    mapping.update(
        {
            DispatchQueue._inflight_member_field(task_key): count
            for task_key, count in inflight_counts.items()
            if count > 0
        }
    )
    return mapping


class TaskMembers:
    """Maintenance for the rebuildable per-task membership hash of one namespace.

    All operations bind to a queue class (``queue_cls``) the same way the
    admission/reservation/lifecycle helpers do. The hash mirrors
    pending/inflight ZSET membership per task and is repaired out of band by
    ``dispatch_task_members_maintenance`` when it drifts or expires.
    """

    @classmethod
    def _rebuilt_key(cls, queue_cls: type[DispatchQueue]) -> str:
        return f"dispatch:{queue_cls._ns()}:task_members_rebuilt"

    @classmethod
    def _rebuild_requested_key(cls, queue_cls: type[DispatchQueue]) -> str:
        return f"dispatch:{queue_cls._ns()}:task_members_rebuild_requested"

    @classmethod
    def _rebuild_attempt_key(cls, queue_cls: type[DispatchQueue]) -> str:
        return f"dispatch:{queue_cls._ns()}:task_members_rebuild_attempt"

    @classmethod
    def hard_rebuild_due(cls, queue_cls: type[DispatchQueue]) -> bool:
        """True when the daily hard-audit safety window has expired."""
        try:
            return not bool(RedisConn.exists(cls._rebuilt_key(queue_cls)))
        except Exception as exc:
            logger.warning(
                "dispatch: task_members hard-rebuild check failed namespace=%s: %s",
                queue_cls._ns(),
                exc,
            )
            return False

    @classmethod
    def rebuild_requested(cls, queue_cls: type[DispatchQueue]) -> bool:
        try:
            return bool(RedisConn.exists(cls._rebuild_requested_key(queue_cls)))
        except Exception as exc:
            logger.warning(
                "dispatch: task_members rebuild-request check failed namespace=%s: %s",
                queue_cls._ns(),
                exc,
            )
            return False

    @classmethod
    def request_rebuild(cls, queue_cls: type[DispatchQueue], reason: str) -> bool:
        """Request out-of-band repair without scanning from the pump."""
        try:
            RedisConn.set(
                cls._rebuild_requested_key(queue_cls),
                str(reason or "unspecified"),
                ex=TASK_MEMBERS_REBUILD_REQUEST_TTL_SECONDS,
            )
            return True
        except Exception as exc:
            logger.warning(
                "dispatch: task_members rebuild request failed namespace=%s reason=%s: %s",
                queue_cls._ns(),
                reason,
                exc,
            )
            return False

    @classmethod
    def try_start_rebuild(cls, queue_cls: type[DispatchQueue]) -> bool:
        """Apply one-hour retry backoff before an independent full scan."""
        return set_redis_ttl_marker(
            cls._rebuild_attempt_key(queue_cls),
            TASK_MEMBERS_REBUILD_RETRY_SECONDS,
            nx=True,
        )

    @classmethod
    def mark_rebuilt(cls, queue_cls: type[DispatchQueue]) -> bool:
        """Commit successful daily audit state and clear request/backoff."""
        try:
            pipe = RedisConn.pipeline(transaction=False)
            pipe.set(cls._rebuilt_key(queue_cls), "1", ex=TASK_MEMBERS_REBUILD_FORCE_SECONDS)
            pipe.delete(
                cls._rebuild_requested_key(queue_cls),
                cls._rebuild_attempt_key(queue_cls),
            )
            pipe.execute()
            return True
        except Exception as exc:
            logger.warning(
                "dispatch: task_members rebuild marker update failed namespace=%s: %s",
                queue_cls._ns(),
                exc,
            )
            return False

    @classmethod
    def counts_drifted(cls, queue_cls: type[DispatchQueue]) -> bool:
        """Cheap drift check: ``ZCARD`` vs hash field sums (no per-task ZSCAN)."""
        try:
            pending_z = queue_cls.pending_count()
            inflight_z = queue_cls.inflight_count()
            if pending_z < 0 or inflight_z < 0:
                return False
            raw = RedisConn.hgetall(queue_cls.task_members_key()) or {}
        except Exception as exc:
            logger.warning(
                "dispatch: task_members drift check failed namespace=%s: %s",
                queue_cls._ns(),
                exc,
            )
            return False

        summarized = _summarize_task_members_hash(raw)
        if summarized is None:
            return True
        pending_sum, inflight_sum = summarized
        return abs(pending_sum - pending_z) > 0 or abs(inflight_sum - inflight_z) > 0

    @classmethod
    def _scan_zset(
        cls,
        queue_cls: type[DispatchQueue],
        zset_key: str,
        registered: list[str],
        *,
        deadline_at: Optional[float],
        scan_count: int,
        scan_pause_seconds: float,
    ) -> Optional[tuple[dict[str, int], int, int, int]]:
        """Return counts, pages, scanned members, and unresolved members."""
        counts: dict[str, int] = defaultdict(int)
        pages = 0
        scanned = 0
        unresolved = 0
        cursor = 0
        while True:
            if deadline_at is not None and time.monotonic() >= deadline_at:
                return None
            cursor, members = RedisConn.zscan(zset_key, cursor, count=max(1, int(scan_count)))
            pages += 1
            scanned += len(members or [])
            for raw_id, _score in members or []:
                task_key = resolve_task_key_from_job_id(_text(raw_id), registered)
                if task_key:
                    counts[task_key] += 1
                else:
                    unresolved += 1
                    counts[UNRESOLVED_TASK_KEY] += 1
            if cursor == 0:
                return dict(counts), pages, scanned, unresolved
            pause = max(0.0, float(scan_pause_seconds))
            if pause:
                time.sleep(pause)

    @classmethod
    def _replace_if_current(
        cls,
        queue_cls: type[DispatchQueue],
        mapping: dict[str, int],
        *,
        expected_pending: int,
        expected_inflight: int,
    ) -> bool:
        args: list[object] = [
            expected_pending,
            expected_inflight,
            len(mapping),
            TASK_MEMBERS_TTL_SECONDS,
        ]
        for field, value in mapping.items():
            args.extend([field, value])
        return bool(
            _replace_task_members_script(
                keys=[
                    queue_cls.pending_key(),
                    queue_cls.inflight_key(),
                    queue_cls.task_members_key(),
                ],
                args=args,
            )
        )

    @classmethod
    def rebuild(
        cls,
        queue_cls: type[DispatchQueue],
        *,
        deadline_at: Optional[float] = None,
        scan_count: int = TASK_MEMBERS_REBUILD_SCAN_COUNT,
        scan_pause_seconds: float = TASK_MEMBERS_REBUILD_SCAN_PAUSE_SECONDS,
    ) -> Optional[dict[str, int]]:
        """Rebuild ``task_members`` Hash from pending/inflight ZSET membership.

        Runs only in independent maintenance. The final Lua replacement checks
        current ZCARD totals against scanned totals, so common concurrent queue
        changes abort without overwriting the live derived hash.
        """
        started_at = time.monotonic()
        try:
            registered = [_text(key) for key in (RedisConn.hkeys(KEY_REGISTERED) or [])]
        except Exception:
            registered = []

        try:
            pending_scan = cls._scan_zset(
                queue_cls,
                queue_cls.pending_key(),
                registered,
                deadline_at=deadline_at,
                scan_count=scan_count,
                scan_pause_seconds=scan_pause_seconds,
            )
            inflight_scan = cls._scan_zset(
                queue_cls,
                queue_cls.inflight_key(),
                registered,
                deadline_at=deadline_at,
                scan_count=scan_count,
                scan_pause_seconds=scan_pause_seconds,
            )
            if pending_scan is None or inflight_scan is None:
                logger.warning(
                    "dispatch: rebuild_task_member_counts aborted by deadline namespace=%s",
                    queue_cls._ns(),
                )
                return None
            pending_counts, pending_pages, pending_scanned, pending_unresolved = pending_scan
            inflight_counts, inflight_pages, inflight_scanned, inflight_unresolved = inflight_scan
            pages = pending_pages + inflight_pages
            scanned = pending_scanned + inflight_scanned
            unresolved = pending_unresolved + inflight_unresolved
            if unresolved:
                # Bucketed under ``pending:__unresolved__`` / ``inflight:__unresolved__``
                # so the totals still reconcile with ZCARD; the log line is the
                # alert signal for stale membership (e.g. a task was unregistered
                # while its jobs were still queued).
                logger.warning(
                    "dispatch: rebuild task_members unresolved namespace=%s members=%d scanned=%d pages=%d",
                    queue_cls._ns(),
                    unresolved,
                    scanned,
                    pages,
                )
            if deadline_at is not None and time.monotonic() >= deadline_at:
                logger.warning(
                    "dispatch: rebuild_task_member_counts aborted by deadline namespace=%s",
                    queue_cls._ns(),
                )
                return None
            mapping = _task_member_mapping(pending_counts, inflight_counts)
            expected_pending = sum(pending_counts.values())
            expected_inflight = sum(inflight_counts.values())
            replaced = cls._replace_if_current(
                queue_cls,
                mapping,
                expected_pending=expected_pending,
                expected_inflight=expected_inflight,
            )
            if not replaced:
                logger.warning(
                    "dispatch: rebuild task_members queue changed namespace=%s scanned=%d pages=%d",
                    queue_cls._ns(),
                    scanned,
                    pages,
                )
                return None
            logger.info(
                "dispatch: rebuilt task_members namespace=%s scanned=%d pages=%d duration=%.3fs fields=%d",
                queue_cls._ns(),
                scanned,
                pages,
                time.monotonic() - started_at,
                len(mapping),
            )
            return dict(mapping)
        except Exception as exc:
            logger.warning("dispatch: rebuild_task_member_counts failed namespace=%s: %s", queue_cls._ns(), exc)
            return None

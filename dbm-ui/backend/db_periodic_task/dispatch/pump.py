# -*- coding: utf-8 -*-
"""
TencentBlueKing is pleased to support the open source community by making 蓝鲸智云-DB管理系统(BlueKing-BK-DBM) available.
Copyright (C) 2017-2023 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at https://opensource.org/licenses/MIT
Unless required by applicable law or agreed to writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
"""

import logging
import math
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Optional

from backend.db_periodic_task.dispatch import routing
from backend.db_periodic_task.dispatch.config import (
    PUMP_CLEANUP_INTERVAL_SECONDS,
    PUMP_INTERVAL_SECONDS,
    DispatchPumpConfig,
    DispatchQueueConfig,
)
from backend.db_periodic_task.dispatch.controller import PumpControlDecision, PumpController
from backend.db_periodic_task.dispatch.job import DispatchJob
from backend.db_periodic_task.dispatch.lifecycle import QueueLifecycle
from backend.db_periodic_task.dispatch.lua import RELEASE_LOCK_LUA, compile_script, eval_script
from backend.db_periodic_task.dispatch.metrics import DispatchMetrics, _text, tick_id
from backend.db_periodic_task.dispatch.queue import DispatchQueue, set_redis_ttl_marker
from backend.db_periodic_task.dispatch.reaper import OrphanReaper
from backend.db_periodic_task.dispatch.registry import dispatch_execute_job, register_failure_handlers
from backend.db_periodic_task.dispatch.reservation import (
    RESERVATION_PIPELINE_CHUNK_SIZE,
    QueueReservation,
    ReservationStatus,
)
from backend.db_periodic_task.dispatch.task_members import TaskMembers
from backend.db_periodic_task.register import register_periodic_task

logger = logging.getLogger("root")

PUMP_LOCK_KEY_PREFIX = "dispatch:{ns}:pump_lock"
PUMP_CLEANUP_KEY_PREFIX = "dispatch:{ns}:pump_cleanup"
# Occupies ``dispatch:{ns}:pump_lock`` so SET NX acquisition fails until resume / TTL.
PUMP_PAUSE_OWNER = "dispatch:paused"
# Pause/resume advances this baseline so intentional downtime is not counted as ``pump_missed``.
PUMP_MISSED_BASELINE_KEY_PREFIX = "dispatch:{ns}:pump_missed_baseline"
PUMP_MISSED_BASELINE_TTL_SECONDS = 24 * 3600

_release_lock_script = compile_script(RELEASE_LOCK_LUA)


def _pump_lock_key(namespace: str) -> str:
    return PUMP_LOCK_KEY_PREFIX.format(ns=namespace)


def _pump_missed_baseline_key(namespace: str) -> str:
    return PUMP_MISSED_BASELINE_KEY_PREFIX.format(ns=namespace)


def _mark_pump_missed_baseline(namespace: str, *, current_tick: Optional[int] = None, client=None) -> None:
    """Ignore missed ticks at or before ``current_tick`` (pause / resume)."""
    try:
        client = client or routing.conn_for_namespace(namespace)
        client.set(
            _pump_missed_baseline_key(namespace),
            int(tick_id() if current_tick is None else current_tick),
            ex=PUMP_MISSED_BASELINE_TTL_SECONDS,
        )
    except Exception as exc:
        logger.debug("dispatch_global_pump[%s]: missed baseline write failed: %s", namespace, exc)


def _read_pump_missed_baseline(namespace: str, *, client=None) -> int:
    try:
        raw = (client or routing.conn_for_namespace(namespace)).get(_pump_missed_baseline_key(namespace))
    except Exception:
        return -1
    if raw is None:
        return -1
    try:
        return int(_text(raw))
    except (TypeError, ValueError):
        return -1


def _record_pump_missed_ticks(queue_cls: type[DispatchQueue], current_tick_id: int) -> int:
    """Backfill empty pump slots since the last decide (excluding pause windows)."""
    state = PumpController.read_state(queue_cls.namespace)
    try:
        last_decide = int(state.get("tick_id", -1))
    except (TypeError, ValueError):
        last_decide = -1
    baseline = max(last_decide, _read_pump_missed_baseline(queue_cls.namespace))
    if baseline < 0:
        return 0
    missed = max(0, int(current_tick_id) - baseline - 1)
    if missed:
        DispatchMetrics.record_queue_counter(
            queue_cls.namespace,
            "pump_missed",
            missed,
            timestamp=current_tick_id * PUMP_INTERVAL_SECONDS,
        )
        logger.info(
            "dispatch_global_pump[%s]: pump_missed=%d current_tick=%d baseline=%d",
            queue_cls.namespace,
            missed,
            current_tick_id,
            baseline,
        )
    return missed


def pause_queue_pump(namespace: str, *, seconds: Optional[float] = None, alias: Optional[str] = None) -> dict:
    """Hold the per-namespace pump lock so ``dispatch_global_pump`` skips this queue.

    ``seconds=None`` keeps the pause until ``resume_queue_pump`` (no Redis TTL).
    Otherwise the pause auto-expires after ``ceil(seconds)`` (≥1).
    ``alias`` pins the Redis shard explicitly (used by remap so the pause lands
    on the old shard even while the route row is about to flip).
    """
    ns = namespace or ""
    if not ns:
        raise ValueError("namespace is required to pause a queue pump")
    key = _pump_lock_key(ns)
    client = routing.conn_for_alias(alias) if alias else routing.conn_for_namespace(ns)
    _mark_pump_missed_baseline(ns, client=client)  # setting baseline here matters less than resuming pump
    if seconds is None:
        client.set(key, PUMP_PAUSE_OWNER)
        logger.warning("dispatch_global_pump[%s]: paused until resume", ns)
        return {"namespace": ns, "paused": True, "ttl_seconds": None}
    if float(seconds) <= 0:
        raise ValueError("seconds must be positive; use seconds=None to pause until resume")
    ttl = max(1, int(math.ceil(float(seconds))))
    client.set(key, PUMP_PAUSE_OWNER, ex=ttl)
    logger.warning("dispatch_global_pump[%s]: paused for %ss", ns, ttl)
    return {"namespace": ns, "paused": True, "ttl_seconds": ttl}


def resume_queue_pump(namespace: str, *, alias: Optional[str] = None) -> bool:
    """Clear a pause marker on the pump lock. Returns whether a pause key was removed."""
    ns = namespace or ""
    if not ns:
        raise ValueError("namespace is required to resume a queue pump")
    client = routing.conn_for_alias(alias) if alias else routing.conn_for_namespace(ns)
    removed = bool(
        eval_script(
            _release_lock_script,
            client=client,
            keys=[_pump_lock_key(ns)],
            args=[PUMP_PAUSE_OWNER],
        )
    )
    # Always advance baseline on resume so pause downtime is not counted as starvation.
    _mark_pump_missed_baseline(ns, client=client)
    if removed:
        logger.warning("dispatch_global_pump[%s]: resumed", ns)
    return removed


def inspect_queue_pump_lock(namespace: str, *, alias: Optional[str] = None) -> dict:
    """Inspect who holds ``dispatch:{ns}:pump_lock``.

    Returns::

        {
            "namespace": "...",
            "key": "dispatch:{ns}:pump_lock",
            "held": bool,
            "owner": str | None,          # raw Redis value
            "state": "free" | "paused" | "pumping" | "held",
            "ttl_seconds": int | None,    # -1 = no expiry; None = missing
        }
    """
    ns = namespace or ""
    key = _pump_lock_key(ns) if ns else ""
    empty = {
        "namespace": ns,
        "key": key,
        "held": False,
        "owner": None,
        "state": "free",
        "ttl_seconds": None,
    }
    if not ns:
        return empty
    try:
        raw = (routing.conn_for_alias(alias) if alias else routing.conn_for_namespace(ns)).get(key)
    except Exception:
        return empty
    if raw is None:
        return empty
    owner = _text(raw)
    try:
        ttl = int((routing.conn_for_alias(alias) if alias else routing.conn_for_namespace(ns)).ttl(key))
    except Exception:
        ttl = None
    else:
        # redis: -2 = key missing (race)
        if ttl == -2:
            return empty
    if owner == PUMP_PAUSE_OWNER:
        state = "paused"
    elif owner.startswith("pump:"):
        state = "pumping"
    else:
        state = "held"
    return {
        "namespace": ns,
        "key": key,
        "held": True,
        "owner": owner,
        "state": state,
        "ttl_seconds": ttl,
    }


def is_queue_pump_paused(namespace: str, *, alias: Optional[str] = None) -> bool:
    """Whether the namespace pump lock is currently held by a pause marker."""
    return inspect_queue_pump_lock(namespace, alias=alias)["state"] == "paused"


def queue_pump_pause_ttl(namespace: str, *, alias: Optional[str] = None) -> Optional[int]:
    """Remaining pause TTL in seconds.

    ``None`` when not paused. ``-1`` when paused with no expiry (until resume).
    """
    info = inspect_queue_pump_lock(namespace, alias=alias)
    if info["state"] != "paused":
        return None
    return info["ttl_seconds"]


@dataclass
class _PumpTickStats:
    candidates: int = 0
    reserved: int = 0
    dispatched: int = 0
    blocked: int = 0
    missing: int = 0
    publish_failed: int = 0
    queue_wait_samples: list[tuple[str, float]] = field(default_factory=list)


def _cleanup_due(queue_cls: type[DispatchQueue], interval_seconds: int) -> bool:
    return set_redis_ttl_marker(
        PUMP_CLEANUP_KEY_PREFIX.format(ns=queue_cls.namespace),
        interval_seconds,
        nx=True,
        client=queue_cls.conn(),
    )


def _maybe_cleanup_queue(queue_cls: type[DispatchQueue], deadline_at: float) -> None:
    """Run bounded cleanup and request any unbounded repair out of band."""
    if time.monotonic() >= deadline_at:
        return
    if not _cleanup_due(queue_cls, PUMP_CLEANUP_INTERVAL_SECONDS):
        return
    orphaned = OrphanReaper.reap_orphaned_queue_members(queue_cls, deadline_at=deadline_at)
    if orphaned:
        logger.info("dispatch_global_pump[%s]: reaped_orphaned=%d", queue_cls.namespace, orphaned)
    if time.monotonic() >= deadline_at:
        logger.info("dispatch_global_pump[%s]: cleanup hit deadline, skip drift check", queue_cls.namespace)
        return
    drifted = TaskMembers.counts_drifted(queue_cls)
    if orphaned or drifted:
        reason = "orphaned" if orphaned else "count_drift"
        TaskMembers.request_rebuild(queue_cls, reason)


def _select_dispatch_candidates(
    queue_cls: type[DispatchQueue],
    job_ids: list[str],
    budget: int,
) -> tuple[list[DispatchJob], int]:
    """Hydrate peeked jobs, discard missing/expired, trim to budget."""
    jobs = queue_cls.get_jobs(job_ids)
    candidates: list[DispatchJob] = []
    missing_count = 0
    now = time.time()
    for job_id in job_ids:
        job = jobs.get(job_id)
        if not job:
            missing_count += 1
            OrphanReaper.discard_orphaned_job(queue_cls, job_id, namespace=queue_cls.namespace)
            continue
        if job.queue_deadline_at and job.queue_deadline_at <= now:
            OrphanReaper.discard_orphaned_job(
                queue_cls,
                job_id,
                task_key=job.task_key,
                namespace=queue_cls.namespace,
            )
            continue
        candidates.append(job)
    return candidates[:budget], missing_count


def _requeue_reserved_tail(
    queue_cls: type[DispatchQueue],
    chunk: list[DispatchJob],
    statuses: list[ReservationStatus],
    from_index: int,
) -> None:
    for pending_job, pending_status in zip(chunk[from_index:], statuses[from_index:]):
        if pending_status == ReservationStatus.RESERVED:
            QueueLifecycle.requeue(
                queue_cls=queue_cls,
                job=pending_job,
                execute_at=pending_job.execute_at,
                job_ttl=queue_cls.resolve_queue_wait_ttl_from_job(pending_job),
            )


def _dispatch_candidates(
    queue_cls: type[DispatchQueue],
    candidates: list[DispatchJob],
    config: DispatchQueueConfig,
    decision: PumpControlDecision,
    deadline_at: float,
    current_tick_id: int,
    stats: _PumpTickStats,
) -> None:
    """Reserve and Celery-publish candidates until blocked, deadline, or publish failure."""
    blocked = False
    inflight_ttl_cache: dict[tuple[str, str], int] = {}
    for offset in range(0, len(candidates), RESERVATION_PIPELINE_CHUNK_SIZE):
        if blocked or time.monotonic() >= deadline_at:
            break
        chunk = candidates[offset : offset + RESERVATION_PIPELINE_CHUNK_SIZE]
        record_ttls = []
        for job in chunk:
            config_identity = (job.task_key, job.config_json)
            if config_identity not in inflight_ttl_cache:
                inflight_ttl_cache[config_identity] = queue_cls.resolve_inflight_ttl_from_job(job)
            record_ttls.append(inflight_ttl_cache[config_identity])
        statuses = QueueReservation.reserve_jobs(
            chunk,
            config,
            queue_cls=queue_cls,
            record_ttls=record_ttls,
            tick_id=current_tick_id,
            tick_budget=decision.effective_budget,
        )
        stats.reserved += sum(status == ReservationStatus.RESERVED for status in statuses)
        stats.blocked += sum(status == ReservationStatus.BLOCKED for status in statuses)
        stats.missing += sum(status == ReservationStatus.MISSING for status in statuses)

        for index, (job, status) in enumerate(zip(chunk, statuses)):
            if status == ReservationStatus.MISSING:
                continue
            if status == ReservationStatus.BLOCKED:
                blocked = True
                continue
            eligible_at = max(job.created_at, job.execute_at)
            stats.queue_wait_samples.append((job.sample_identity, max(0.0, time.time() - eligible_at)))
            try:
                dispatch_execute_job.apply_async(args=[job.job_id])
            except Exception as exc:
                stats.publish_failed += 1
                DispatchMetrics.record_task_counter(queue_cls._ns(), job.task_key, "publish_failed")
                _requeue_reserved_tail(queue_cls, chunk, statuses, index)
                logger.warning(
                    "dispatch_global_pump[%s]: publish failed job_id=%s: %s",
                    queue_cls.namespace,
                    job.job_id,
                    exc,
                )
                return
            stats.dispatched += 1


def _flush_pump_metrics(
    queue_cls: type[DispatchQueue],
    stats: _PumpTickStats,
    metric_timestamp: float,
    started_at: float,
) -> None:
    try:
        pipe = queue_cls.conn().pipeline(transaction=False)
        for name, amount in (
            ("candidates", stats.candidates),
            ("reserved", stats.reserved),
            ("dispatched", stats.dispatched),
            ("blocked", stats.blocked),
            ("missing", stats.missing),
            ("publish_failed", stats.publish_failed),
        ):
            if amount:
                DispatchMetrics.record_queue_counter(
                    queue_cls.namespace,
                    name,
                    amount,
                    timestamp=metric_timestamp,
                    client=pipe,
                )
        DispatchMetrics.record_samples(
            queue_cls.namespace,
            "queue_wait_seconds",
            stats.queue_wait_samples,
            timestamp=metric_timestamp,
            client=pipe,
        )
        DispatchMetrics.record_sample(
            queue_cls.namespace,
            "pump_seconds",
            time.monotonic() - started_at,
            client=pipe,
        )
        pipe.execute()
    except Exception as exc:
        logger.debug("dispatch_global_pump[%s]: metrics flush failed: %s", queue_cls.namespace, exc)


def _pump_queue(queue_cls: type[DispatchQueue], deadline_at: float, current_tick_id: int) -> int:
    """Drain one FIFO queue up to its adaptive tick budget and the global deadline."""
    started_at = time.monotonic()
    metric_timestamp = current_tick_id * PUMP_INTERVAL_SECONDS
    stats = _PumpTickStats()
    if time.monotonic() >= deadline_at:
        return 0
    config = queue_cls.load_config()
    _maybe_cleanup_queue(queue_cls, deadline_at)
    _record_pump_missed_ticks(queue_cls, current_tick_id)

    try:
        decision = PumpController.decide(queue_cls, config, current_tick_id=current_tick_id)
        if decision.effective_budget <= 0:
            return 0
        job_ids = queue_cls.peek_eligible(max(10, decision.effective_budget * 3))
        stats.candidates = len(job_ids)
        if not job_ids:
            return 0
        candidates, missing = _select_dispatch_candidates(queue_cls, job_ids, decision.effective_budget)
        stats.missing = missing
        _dispatch_candidates(queue_cls, candidates, config, decision, deadline_at, current_tick_id, stats)
        logger.info(
            "dispatch_global_pump[%s]: candidates=%d budget=%d cwnd=%d flow=%d dispatched=%d aimd=%s",
            queue_cls.namespace,
            stats.candidates,
            decision.effective_budget,
            decision.congestion_window,
            decision.flow_window,
            stats.dispatched,
            decision.aimd_action,
        )
        return stats.dispatched
    finally:
        _flush_pump_metrics(queue_cls, stats, metric_timestamp, started_at)


def _try_acquire_pump_lock(namespace: str, owner: str, ttl_seconds: int) -> bool:
    try:
        return bool(
            routing.conn_for_namespace(namespace).set(
                _pump_lock_key(namespace),
                owner,
                nx=True,
                ex=max(1, int(ttl_seconds)),
            )
        )
    except Exception as exc:
        logger.warning("dispatch_global_pump[%s]: lock acquisition failed: %s", namespace, exc)
        return False


def _release_pump_lock(namespace: str, owner: str) -> None:
    try:
        eval_script(
            _release_lock_script,
            client=routing.conn_for_namespace(namespace),
            keys=[_pump_lock_key(namespace)],
            args=[owner],
        )
    except Exception as exc:
        logger.warning("dispatch_global_pump[%s]: lock release failed: %s", namespace, exc)


def _pump_queue_with_lock(
    queue_cls: type[DispatchQueue],
    deadline_at: float,
    current_tick_id: int,
    lock_ttl_seconds: int,
) -> int:
    """Acquire the per-namespace pump lock, then drain that queue once."""
    owner = f"pump:{queue_cls.namespace}:{uuid.uuid4().hex}"
    if not _try_acquire_pump_lock(queue_cls.namespace, owner, lock_ttl_seconds):
        if is_queue_pump_paused(queue_cls.namespace):
            logger.info("dispatch_global_pump[%s]: paused, skip", queue_cls.namespace)
        else:
            logger.debug("dispatch_global_pump[%s]: lock contention, skip", queue_cls.namespace)
            DispatchMetrics.record_queue_counter(
                queue_cls.namespace,
                "pump_lock_skip",
                timestamp=current_tick_id * PUMP_INTERVAL_SECONDS,
            )
        return 0
    try:
        return _pump_queue(queue_cls, deadline_at, current_tick_id)
    finally:
        _release_pump_lock(queue_cls.namespace, owner)


@register_periodic_task(run_every=timedelta(seconds=PUMP_INTERVAL_SECONDS))
def dispatch_global_pump():
    """Drain registered queues with per-namespace locks.

    Each Celery worker may pump different namespaces in the same tick. The lock
    is namespace-scoped so multi-worker fleets actually parallelize across queues;
    ``max_parallel_queues`` only caps in-process thread concurrency.
    """
    queues = DispatchQueue.iter_queues()
    if not queues:
        return 0
    pump_config = DispatchPumpConfig()
    current_tick_id = tick_id()
    deadline_at = time.monotonic() + pump_config.deadline_seconds
    lock_ttl_seconds = pump_config.lock_ttl_seconds
    total = 0
    worker_count = min(len(queues), max(1, int(pump_config.max_parallel_queues)))
    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="dispatch-pump") as executor:
        queue_iter = iter(queues)
        futures = {}

        def submit_next() -> bool:
            if time.monotonic() >= deadline_at:
                return False
            try:
                queue_cls = next(queue_iter)
            except StopIteration:
                return False
            futures[
                executor.submit(
                    _pump_queue_with_lock,
                    queue_cls,
                    deadline_at,
                    current_tick_id,
                    lock_ttl_seconds,
                )
            ] = queue_cls
            return True

        for _ in range(worker_count):
            if not submit_next():
                break
        while futures:
            completed, _ = wait(futures, return_when=FIRST_COMPLETED)
            for future in completed:
                queue_cls = futures.pop(future)
                try:
                    total += future.result()
                except Exception as exc:
                    logger.warning("dispatch_global_pump[%s]: failed: %s", queue_cls.namespace, exc)
                submit_next()
    return total


register_failure_handlers()

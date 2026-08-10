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
import uuid
from datetime import timedelta
from typing import Any, Optional

from backend.db_periodic_task.dispatch import routing
from backend.db_periodic_task.dispatch.lua import RELEASE_LOCK_LUA, compile_script, eval_script
from backend.db_periodic_task.dispatch.observability import (
    DEFAULT_REPORT_WINDOW_SECONDS,
    DispatchStats,
    QueueDispatchReport,
)
from backend.db_periodic_task.register import register_periodic_task

"""Periodic publisher that turns bounded dispatch stats into a compact
Prometheus-facing JSON snapshot on global Redis.

The dispatch layer already aggregates queue/task counters, latency reservoirs
and live queue state in ``DispatchStats.snapshot()`` (bounded 25h Redis
retention). This module is the single writer that collapses that data into a
low-cardinality payload the Prometheus Collector
(``backend.bk_dataview.prometheus.dispatch_metrics``) can scrape cheaply.

Design notes (metric contract, see OpenSpec add-dispatch-prometheus-metrics):

- One writer: a global-Redis owner-safe lock (SET NX EX, Lua owner-verified
  release — the same pattern as the pump lock) keeps a single Celery worker
  publishing at any moment. The TTL is 2x the publish interval so a crashed
  holder self-heals on expiry.
- Two refresh tiers: realtime + 1h are refreshed every 30s; the 24h window is
  only recomputed when the cached 24h ``generated_at`` is older than 5 minutes
  (the value is read from Redis, not from process memory, so a hand-off to
  another worker never forces an early recompute).
- Fail-open: a 1h snapshot failure leaves the existing cache untouched; a 24h
  recompute failure reuses the previous 24h section. ``generated_at`` is
  preserved per window so the Collector can report staleness independently.
- Compact payload: only the values the Collector needs (live counts, capacity,
  counters, p50/p95/p99, per-task outcomes). Reservoir ``samples`` arrays and
  the full ``registered`` metadata are deliberately omitted.
- Cardinally bounded: task series are sorted by ``task_key`` and capped at
  ``MAX_TASK_EXPORTS``; overflows are logged and the affected namespaces are
  flagged ``partial`` instead of emitting unbounded time series.

Payload schema (``SCHEMA_VERSION``)::

    {
      "schema_version": 1,
      "generated_at": {"1h": <epoch|null>, "24h": <epoch|null>},
      "queues": [   # live/capacity/status per namespace
        {"namespace", "pending", "pending_ready", "pending_delaying",
         "inflight", "max_admitted_jobs", "max_inflight", "budget",
         "flow_window", "congestion_window",
         "pump_paused", "producer_paused", "inflight_saturated"}
      ],
      "tasks": [    # live per-task pending/inflight
        {"task_key", "namespace", "pending", "inflight"}
      ],
      "windows": {  # per-window aggregates
        "1h"|"24h": {
          "generated_at": <epoch>,
          "queues": {ns: {"counters": {...}, "latency": {stage: {p50,p95,p99}}}},
          "partial": {ns: 0|1},
          "tasks":  {task_key: {"namespace", "outcomes": {...}, "partial": 0|1}}
        }
      }
    }

Missing/unavailable values are ``null`` (never ``-1``); the Collector skips
them, and ``partial`` marks incomplete windows instead.
"""

logger = logging.getLogger("root")

PUBLISH_INTERVAL_SECONDS = 30
WINDOW_24H_SECONDS = 24 * 60 * 60
WINDOW_24H_REFRESH_SECONDS = 5 * 60
# TTL comfortably beyond the 24h staleness threshold so a dead publisher is
# first reported ``cache_stale`` (data present but old) and only later
# ``cache_miss`` (key expired), instead of jumping straight to missing.
LATEST_KEY_TTL_SECONDS = 15 * 60

# Global Redis keys (``dispatch:prometheus:*`` stays on the default shard,
# next to ``dispatch:registered``).
KEY_LATEST = "dispatch:prometheus:latest"
KEY_HEARTBEAT = "dispatch:prometheus:publisher_heartbeat"
KEY_PUBLISHER_LOCK = "dispatch:prometheus:publisher_lock"

PUBLISHER_LOCK_TTL_SECONDS = 60  # 2x publish interval; crash recovers on TTL expiry
PUBLISHER_LOCK_OWNER_PREFIX = "stats_publisher:"

SCHEMA_VERSION = 1
MAX_TASK_EXPORTS = 500
PAYLOAD_WARN_BYTES = 1024 * 1024

LATENCY_STAGES = ("queue_wait_seconds", "execution_seconds", "pump_seconds")
WINDOWS = ("1h", "24h")

_release_lock_script = compile_script(RELEASE_LOCK_LUA)


def _nonneg(value: Any) -> Optional[int]:
    """Coerce to a non-negative int, or ``None`` for missing/-1 (unavailable)."""
    try:
        value = int(value)
    except (TypeError, ValueError):
        return None
    return value if value >= 0 else None


def _try_acquire_publisher_lock(client, owner: str) -> bool:
    try:
        return bool(client.set(KEY_PUBLISHER_LOCK, owner, nx=True, ex=max(1, int(PUBLISHER_LOCK_TTL_SECONDS))))
    except Exception as exc:
        logger.warning("dispatch stats publisher: lock acquisition failed: %s", exc)
        return False


def _release_publisher_lock(client, owner: str) -> None:
    try:
        eval_script(_release_lock_script, client=client, keys=[KEY_PUBLISHER_LOCK], args=[owner])
    except Exception as exc:
        logger.warning("dispatch stats publisher: lock release failed: %s", exc)


def _update_heartbeat(client, now: float) -> None:
    """Stamp the alive-time key right after lock acquisition."""
    try:
        client.set(KEY_HEARTBEAT, str(now), ex=LATEST_KEY_TTL_SECONDS)
    except Exception as exc:
        logger.warning("dispatch stats publisher: heartbeat update failed: %s", exc)


def _load_latest(client) -> Optional[dict]:
    try:
        raw = client.get(KEY_LATEST)
    except Exception as exc:
        logger.warning("dispatch stats publisher: latest cache read failed: %s", exc)
        return None
    if not raw:
        return None
    try:
        decoded = json.loads(raw)
    except (TypeError, ValueError) as exc:
        logger.warning("dispatch stats publisher: latest cache corrupt: %s", exc)
        return None
    return decoded if isinstance(decoded, dict) else None


def _needs_24h_refresh(now: float, generated_at: Any) -> bool:
    """Whether the cached 24h window is missing or older than 5 minutes."""
    if not generated_at:
        return True
    try:
        return now - float(generated_at) >= WINDOW_24H_REFRESH_SECONDS
    except (TypeError, ValueError):
        return True


def _queue_live(report: QueueDispatchReport) -> dict:
    controller = report.controller or {}
    config = report.config or {}
    diagnosis = report.diagnosis or []
    return {
        "namespace": report.namespace,
        "pending": _nonneg(report.pending_total),
        "pending_ready": _nonneg(report.pending_ready),
        "pending_delaying": _nonneg(report.pending_delaying),
        "inflight": _nonneg(report.inflight),
        "max_admitted_jobs": _nonneg(config.get("max_admitted_jobs")),
        "max_inflight": _nonneg(config.get("max_inflight")),
        "budget": _nonneg(controller.get("effective_budget")),
        "flow_window": _nonneg(controller.get("flow_window")),
        "congestion_window": _nonneg(controller.get("congestion_window")),
        "pump_paused": 1 if (report.pump_lock or {}).get("state") == "paused" else 0,
        "producer_paused": 1 if (report.producer_lock or {}).get("state") == "paused" else 0,
        "inflight_saturated": 1 if "inflight_saturated" in diagnosis else 0,
    }


def _percentiles(dist: Any) -> Optional[dict]:
    """p50/p95/p99 only — reservoir ``samples`` are never exported."""
    if not dist:
        return None
    out: dict[str, float] = {}
    for name in ("p50", "p95", "p99"):
        value = dist.get(name)
        if value is None:
            continue
        try:
            out[name] = float(value)
        except (TypeError, ValueError):
            continue
    return out or None


def _window_section(snapshot: Any) -> dict:
    queues: dict[str, dict] = {}
    partial: dict[str, int] = {}
    for report in snapshot.queues:
        queues[report.namespace] = {
            "counters": dict(report.counters or {}),
            "latency": {name: _percentiles(report.distributions.get(name)) for name in LATENCY_STAGES},
        }
        partial[report.namespace] = 1 if report.partial else 0
    tasks: dict[str, dict] = {}
    for report in snapshot.task_reports:
        tasks[report.task_key] = {
            "namespace": report.namespace,
            "outcomes": dict(report.outcomes or {}),
            "partial": 1 if report.partial else 0,
        }
    return {
        "generated_at": snapshot.timestamp,
        "queues": queues,
        "partial": partial,
        "tasks": tasks,
    }


def _select_allowed_tasks(snapshot_1h: Any) -> tuple[set, set]:
    """Sorted task keys capped at ``MAX_TASK_EXPORTS``, plus truncated namespaces."""
    keys = {report.task_key for report in snapshot_1h.task_reports}
    ordered = sorted(keys)
    if len(ordered) <= MAX_TASK_EXPORTS:
        return set(ordered), set()
    dropped = set(ordered[MAX_TASK_EXPORTS:])
    dropped_ns = {report.namespace for report in snapshot_1h.task_reports if report.task_key in dropped}
    logger.warning(
        "dispatch stats publisher: task export capped at %d (registered %d); truncating %d task(s)",
        MAX_TASK_EXPORTS,
        len(ordered),
        len(dropped),
    )
    return set(ordered[:MAX_TASK_EXPORTS]), dropped_ns


def build_payload(snapshot_1h: Any, section_24h: Optional[dict]) -> dict:
    """Assemble the compact Prometheus payload from a fresh 1h snapshot and an
    optional 24h window section (fresh recompute or previous successful data)."""
    allowed, truncated_ns = _select_allowed_tasks(snapshot_1h)
    section_1h = _window_section(snapshot_1h)
    sections: dict[str, Optional[dict]] = {"1h": section_1h, "24h": section_24h}
    windows: dict[str, Optional[dict]] = {}
    for window, section in sections.items():
        if section is None:
            windows[window] = None
            continue
        section["tasks"] = {task_key: data for task_key, data in section["tasks"].items() if task_key in allowed}
        for ns in truncated_ns:
            if ns in section["partial"]:
                section["partial"][ns] = 1
        windows[window] = section

    tasks = [
        {
            "task_key": report.task_key,
            "namespace": report.namespace,
            "pending": _nonneg(report.pending),
            "inflight": _nonneg(report.inflight),
        }
        for report in snapshot_1h.task_reports
        if report.task_key in allowed
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": {
            "1h": section_1h.get("generated_at"),
            "24h": (section_24h or {}).get("generated_at") if isinstance(section_24h, dict) else None,
        },
        "queues": [_queue_live(report) for report in snapshot_1h.queues],
        "tasks": tasks,
        "windows": windows,
    }


def publish_dispatch_stats() -> None:
    """One publish cycle: lock, heartbeat, refresh 1h (+24h when stale), SET."""
    now = time.time()
    client = routing.global_conn()
    owner = f"{PUBLISHER_LOCK_OWNER_PREFIX}{uuid.uuid4().hex}"
    if not _try_acquire_publisher_lock(client, owner):
        logger.info("dispatch stats publisher: another process holds the lock; skip")
        return
    try:
        _update_heartbeat(client, now)

        try:
            snapshot_1h = DispatchStats.snapshot(window_seconds=DEFAULT_REPORT_WINDOW_SECONDS)
        except Exception:
            logger.exception("dispatch stats publisher: 1h snapshot failed; keeping existing cache")
            return

        existing = _load_latest(client)
        old_24h = None
        if isinstance(existing, dict):
            old_24h = (existing.get("windows") or {}).get("24h")
        old_24h_generated_at = None
        if isinstance(old_24h, dict):
            old_24h_generated_at = old_24h.get("generated_at")

        if _needs_24h_refresh(now, old_24h_generated_at):
            try:
                section_24h = _window_section(DispatchStats.snapshot(window_seconds=WINDOW_24H_SECONDS))
            except Exception:
                logger.exception("dispatch stats publisher: 24h refresh failed; reusing previous 24h data")
                section_24h = old_24h if isinstance(old_24h, dict) else None
        else:
            section_24h = old_24h if isinstance(old_24h, dict) else None

        payload = build_payload(snapshot_1h, section_24h)
        blob = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        byte_size = len(blob.encode("utf-8"))
        if byte_size > PAYLOAD_WARN_BYTES:
            logger.warning(
                "dispatch stats publisher: payload exceeds %d bytes (%d)",
                PAYLOAD_WARN_BYTES,
                byte_size,
            )
        client.set(KEY_LATEST, blob, ex=LATEST_KEY_TTL_SECONDS)
    finally:
        _release_publisher_lock(client, owner)


@register_periodic_task(run_every=timedelta(seconds=PUBLISH_INTERVAL_SECONDS))
def dispatch_publish_stats():
    """Celery beat entrypoint: publish dispatch stats to the Prometheus cache."""
    try:
        publish_dispatch_stats()
    except Exception:
        logger.exception("dispatch stats publisher: unexpected failure")

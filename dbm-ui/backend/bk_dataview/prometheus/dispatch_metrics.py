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
from typing import Any, Optional

from prometheus_client.core import GaugeMetricFamily

logger = logging.getLogger("root")

# Global Redis cache written by ``db_periodic_task.dispatch.stats_publisher``.
KEY_LATEST = "dispatch:prometheus:latest"
KEY_HEARTBEAT = "dispatch:prometheus:publisher_heartbeat"
EXPORT_LEASE_PREFIX = "dispatch:prometheus:export_lease:"
EXPORT_SLOT_SECONDS = 30
EXPORT_LEASE_TTL_SECONDS = 30

# Freshness thresholds: 2x the publisher refresh cadence per window.
REFRESH_TOLERANCE_SECONDS = {"1h": 60, "24h": 10 * 60}

HEALTH_STATUSES = ("ok", "cache_miss", "cache_stale", "parse_error", "redis_error")
WINDOWS = ("1h", "24h")

# Event/outcome label values come from fixed code-level sets — never from
# unbounded values such as ``job_id``. Unknown values are dropped to keep the
# time-series cardinality bounded.
EVENT_WHITELIST = frozenset(
    {
        "enqueued",
        "candidates",
        "reserved",
        "dispatched",
        "completed",
        "enqueue_duplicate",
        "enqueue_capacity_rejected",
        "enqueue_producer_rejected",
        "enqueue_unavailable",
        "blocked",
        "congestion",
        "missing",
        "publish_failed",
        "celery_failure",
        "pump_missed",
        "pump_lock_skip",
    }
)
OUTCOME_WHITELIST = frozenset(
    {
        "success",
        "timeout_invoke",
        "ratelimit_retry",
        "ratelimit_gave_up",
        "error",
        "skipped",
        "enqueued",
        "enqueue_duplicate",
        "enqueue_capacity_rejected",
        "enqueue_producer_rejected",
        "enqueue_deadline_expired",
        "enqueue_unavailable",
        "expired",
    }
)

# ``(metric_name, payload_key, labelnames)`` for per-namespace live/capacity/status gauges.
_LIVE_GAUGES = (
    ("dbm_dispatch_pending", "pending"),
    ("dbm_dispatch_pending_ready", "pending_ready"),
    ("dbm_dispatch_pending_delaying", "pending_delaying"),
    ("dbm_dispatch_inflight", "inflight"),
    ("dbm_dispatch_max_admitted_jobs", "max_admitted_jobs"),
    ("dbm_dispatch_max_inflight", "max_inflight"),
    ("dbm_dispatch_budget", "budget"),
    ("dbm_dispatch_flow_window", "flow_window"),
    ("dbm_dispatch_congestion_window", "congestion_window"),
    ("dbm_dispatch_pump_paused", "pump_paused"),
    ("dbm_dispatch_producer_paused", "producer_paused"),
    ("dbm_dispatch_inflight_saturated", "inflight_saturated"),
)


class DispatchMetricsCollector:
    """Prometheus Collector reading the dispatch stats payload from global Redis.

    The single publisher (``stats_publisher``) owns writing
    ``dispatch:prometheus:latest``; this collector is a pure reader. No
    module-level ``Gauge`` with the same names is defined — everything is
    generated from the cache via ``GaugeMetricFamily`` so every scrape reflects
    the freshest payload and a removed payload leaves no stale time series.

    Multi-process export is bounded by a per-30s-slot Redis lease
    (``dispatch:prometheus:export_lease:<slot>``, SET NX EX 30): only the
    process that acquires the current slot returns dispatch samples, so a
    Web/Celery fleet pushes one data point per slot instead of N duplicates.
    Every other reporter still emits its existing metrics unchanged.

    Fail-open: cache misses, corrupt JSON, expired data and Redis failures
    never raise into the default REGISTRY. They surface through the one-hot
    ``dbm_dispatch_collector_health`` gauge instead. When Redis is entirely
    unavailable the lease cannot be acquired, so every process reports
    ``redis_error`` — duplicates are the right trade-off over silence there.
    """

    def __init__(self, client: Optional[Any] = None):
        self._client = client

    def describe(self):
        """Declare metric names without touching Redis.

        ``REGISTRY.register`` prefers ``describe()`` over ``collect()`` for
        duplicate-name checks. Returning empty families here keeps registration
        side-effect free (no lease grab, no cache read).
        """
        families = [
            GaugeMetricFamily(
                "dbm_dispatch_collector_health",
                "Dispatch collector health (one-hot over a fixed status set).",
                labels=["status"],
            ),
        ]
        for metric, _key in _LIVE_GAUGES:
            families.append(GaugeMetricFamily(metric, f"Dispatch {metric} (per namespace)", labels=["namespace"]))
        families.extend(
            [
                GaugeMetricFamily(
                    "dbm_dispatch_window_events",
                    "Sliding-window dispatch event counters per namespace.",
                    labels=["namespace", "event", "window"],
                ),
                GaugeMetricFamily(
                    "dbm_dispatch_latency_seconds",
                    "Dispatch latency percentiles per stage.",
                    labels=["namespace", "stage", "quantile", "window"],
                ),
                GaugeMetricFamily(
                    "dbm_dispatch_task_pending",
                    "Pending work items per dispatch task.",
                    labels=["namespace", "task_key"],
                ),
                GaugeMetricFamily(
                    "dbm_dispatch_task_inflight",
                    "Inflight work items per dispatch task.",
                    labels=["namespace", "task_key"],
                ),
                GaugeMetricFamily(
                    "dbm_dispatch_task_outcome",
                    "Outcome counters per dispatch task.",
                    labels=["namespace", "task_key", "outcome", "window"],
                ),
                GaugeMetricFamily(
                    "dbm_dispatch_report_partial",
                    "Dispatch data completeness per namespace per window (1 = incomplete).",
                    labels=["namespace", "window"],
                ),
                GaugeMetricFamily(
                    "dbm_dispatch_refresh_timestamp_seconds",
                    "Unix timestamp of the last successful publish per window.",
                    labels=["window"],
                ),
                GaugeMetricFamily(
                    "dbm_dispatch_publisher_heartbeat_timestamp_seconds",
                    "Unix timestamp of the last publisher lock acquisition (absence = publisher down).",
                    labels=[],
                ),
            ]
        )
        return families

    # ------------------------------------------------------------------ #
    # Redis access
    # ------------------------------------------------------------------ #
    def _get_client(self):
        if self._client is None:
            from django_redis import get_redis_connection

            self._client = get_redis_connection("default")
        return self._client

    def _acquire_export_lease(self, client) -> bool:
        slot = int(time.time()) // EXPORT_SLOT_SECONDS
        key = f"{EXPORT_LEASE_PREFIX}{slot}"
        try:
            return bool(client.set(key, "1", nx=True, ex=EXPORT_LEASE_TTL_SECONDS))
        except Exception as exc:
            logger.warning("dispatch collector: export lease acquisition failed: %s", exc)
            raise

    def _load_payload(self, client) -> tuple[Optional[dict], str]:
        """Return ``(payload, status)``; ``status`` is ``ok``, ``cache_miss`` or ``parse_error``."""
        try:
            raw = client.get(KEY_LATEST)
        except Exception:
            raise
        if raw is None:
            return None, "cache_miss"
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError) as exc:
            logger.warning("dispatch collector: payload corrupt: %s", exc)
            return None, "parse_error"
        if not isinstance(payload, dict):
            return None, "parse_error"
        return payload, "ok"

    # ------------------------------------------------------------------ #
    # Sample generation
    # ------------------------------------------------------------------ #
    def collect(self):
        health = GaugeMetricFamily(
            "dbm_dispatch_collector_health",
            "Dispatch collector health (one-hot over a fixed status set).",
            labels=["status"],
        )
        try:
            client = self._get_client()
            lease_held = self._acquire_export_lease(client)
        except Exception:
            self._set_health(health, "redis_error")
            return [health]
        if not lease_held:
            # Another reporter owns this slot; emit nothing for dispatch.
            return []

        try:
            payload, status = self._load_payload(client)
        except Exception:
            self._set_health(health, "redis_error")
            return [health]
        if status != "ok":
            self._set_health(health, status)
            return [health]

        families = self._build_families(payload)
        self._set_health(health, self._freshness_status(payload))
        return [health] + families

    def _build_families(self, payload: dict) -> list:
        families: list = []
        for metric, key in _LIVE_GAUGES:
            fam = GaugeMetricFamily(metric, f"Dispatch {metric} (per namespace)", labels=["namespace"])
            for queue in payload.get("queues") or []:
                value = queue.get(key)
                if value is None:
                    continue  # unavailable sample: never emit -1
                fam.add_metric([queue.get("namespace") or ""], float(value))
            families.append(fam)

        families.append(self._window_events_family(payload))
        families.append(self._latency_family(payload))
        families.extend(self._task_families(payload))
        families.append(self._partial_family(payload))
        families.append(self._refresh_family(payload))
        families.append(self._heartbeat_family())
        return families

    def _window_sections(self, payload: dict):
        return payload.get("windows") or {}

    def _window_events_family(self, payload: dict) -> GaugeMetricFamily:
        fam = GaugeMetricFamily(
            "dbm_dispatch_window_events",
            "Sliding-window dispatch event counters per namespace.",
            labels=["namespace", "event", "window"],
        )
        for window in WINDOWS:
            section = self._window_sections(payload).get(window)
            if not isinstance(section, dict):
                continue
            for ns, queue_data in (section.get("queues") or {}).items():
                for event, count in (queue_data.get("counters") or {}).items():
                    if event in EVENT_WHITELIST and count is not None:
                        fam.add_metric([ns, event, window], float(count))
        return fam

    def _latency_family(self, payload: dict) -> GaugeMetricFamily:
        fam = GaugeMetricFamily(
            "dbm_dispatch_latency_seconds",
            "Dispatch latency percentiles per stage.",
            labels=["namespace", "stage", "quantile", "window"],
        )
        for window in WINDOWS:
            section = self._window_sections(payload).get(window)
            if not isinstance(section, dict):
                continue
            for ns, queue_data in (section.get("queues") or {}).items():
                for stage, percentiles in (queue_data.get("latency") or {}).items():
                    if not isinstance(percentiles, dict):
                        continue
                    stage_label = stage.removesuffix("_seconds")
                    for quantile, value in percentiles.items():
                        if value is not None:
                            fam.add_metric([ns, stage_label, quantile, window], float(value))
        return fam

    def _task_families(self, payload: dict) -> list:
        pending_fam = GaugeMetricFamily(
            "dbm_dispatch_task_pending",
            "Pending work items per dispatch task.",
            labels=["namespace", "task_key"],
        )
        inflight_fam = GaugeMetricFamily(
            "dbm_dispatch_task_inflight",
            "Inflight work items per dispatch task.",
            labels=["namespace", "task_key"],
        )
        outcome_fam = GaugeMetricFamily(
            "dbm_dispatch_task_outcome",
            "Outcome counters per dispatch task.",
            labels=["namespace", "task_key", "outcome", "window"],
        )
        for task in payload.get("tasks") or []:
            task_key = task.get("task_key") or ""
            namespace = task.get("namespace") or ""
            if task.get("pending") is not None:
                pending_fam.add_metric([namespace, task_key], float(task["pending"]))
            if task.get("inflight") is not None:
                inflight_fam.add_metric([namespace, task_key], float(task["inflight"]))
        for window in WINDOWS:
            section = self._window_sections(payload).get(window)
            if not isinstance(section, dict):
                continue
            for task_key, task_data in (section.get("tasks") or {}).items():
                namespace = task_data.get("namespace") or ""
                for outcome, count in (task_data.get("outcomes") or {}).items():
                    if outcome in OUTCOME_WHITELIST and count is not None:
                        outcome_fam.add_metric([namespace, task_key, outcome, window], float(count))
        return [pending_fam, inflight_fam, outcome_fam]

    def _partial_family(self, payload: dict) -> GaugeMetricFamily:
        fam = GaugeMetricFamily(
            "dbm_dispatch_report_partial",
            "Dispatch data completeness per namespace per window (1 = incomplete).",
            labels=["namespace", "window"],
        )
        for window in WINDOWS:
            section = self._window_sections(payload).get(window)
            if not isinstance(section, dict):
                continue
            for ns, flag in (section.get("partial") or {}).items():
                fam.add_metric([ns, window], float(1 if flag else 0))
        return fam

    def _refresh_family(self, payload: dict) -> GaugeMetricFamily:
        fam = GaugeMetricFamily(
            "dbm_dispatch_refresh_timestamp_seconds",
            "Unix timestamp of the last successful publish per window.",
            labels=["window"],
        )
        generated_at = payload.get("generated_at") or {}
        for window in WINDOWS:
            value = generated_at.get(window)
            if value is not None:
                fam.add_metric([window], float(value))
        return fam

    def _heartbeat_family(self) -> GaugeMetricFamily:
        fam = GaugeMetricFamily(
            "dbm_dispatch_publisher_heartbeat_timestamp_seconds",
            "Unix timestamp of the last publisher lock acquisition (absence = publisher down).",
            labels=[],
        )
        try:
            raw = self._get_client().get(KEY_HEARTBEAT)
        except Exception:
            return fam
        if raw is not None:
            try:
                fam.add_metric([], float(raw))
            except (TypeError, ValueError):
                pass
        return fam

    def _freshness_status(self, payload: dict) -> str:
        generated_at = payload.get("generated_at") or {}
        gen_1h = generated_at.get("1h")
        if gen_1h is None:
            return "cache_stale"
        try:
            age = time.time() - float(gen_1h)
        except (TypeError, ValueError):
            return "cache_stale"
        if age > REFRESH_TOLERANCE_SECONDS["1h"]:
            return "cache_stale"
        return "ok"

    def _set_health(self, family: GaugeMetricFamily, status: str) -> None:
        if status not in HEALTH_STATUSES:
            status = "redis_error"
        for candidate in HEALTH_STATUSES:
            family.add_metric([candidate], 1.0 if candidate == status else 0.0)

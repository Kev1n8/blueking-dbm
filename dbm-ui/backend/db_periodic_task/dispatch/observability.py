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
import sys
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Optional, TextIO

from backend.db_periodic_task.dispatch import routing
from backend.db_periodic_task.dispatch.config import PUMP_INTERVAL_SECONDS, DispatchPumpConfig
from backend.db_periodic_task.dispatch.controller import PumpController
from backend.db_periodic_task.dispatch.metrics import HOUR_SECONDS, METRICS_WINDOW_SECONDS, DispatchMetrics, tick_id
from backend.db_periodic_task.dispatch.queue import KEY_REGISTERED, DispatchQueue

DEFAULT_REPORT_WINDOW_SECONDS = HOUR_SECONDS
MAX_REPORT_WINDOW_SECONDS = METRICS_WINDOW_SECONDS
DISTRIBUTION_NAMES = ("queue_wait_seconds", "execution_seconds", "pump_seconds")
_BAR_WIDTH = 20
_ANSI_CLEAR = "\033[2J\033[H"


def _progress_bar(value: int | float, capacity: int | float, width: int = _BAR_WIDTH) -> str:
    try:
        current = max(0.0, float(value))
        total = float(capacity)
    except (TypeError, ValueError):
        return f"[{'?' * width}]"
    if total <= 0:
        filled = 0
    else:
        filled = max(0, min(width, int(round(width * current / total))))
    return f"[{'#' * filled}{'-' * (width - filled)}]"


def _fmt_int(value: Any, default: str = "?") -> str:
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return default


def _fmt_epoch(timestamp: float | int, default: str = "?") -> str:
    """Local wall-clock time for dashboard headers."""
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(timestamp)))
    except (TypeError, ValueError, OSError, OverflowError):
        return default


def _fmt_tick(tick: int | float, default: str = "?") -> str:
    """Convert pump tick id (epoch // PUMP_INTERVAL_SECONDS) to local time."""
    try:
        return _fmt_epoch(int(tick) * PUMP_INTERVAL_SECONDS, default=default)
    except (TypeError, ValueError):
        return default


def _dash_row(label: str, bar: str = "", value: str = "", note: str = "") -> str:
    """Fixed-width columns so labels/bars/values line up in a monospace terminal."""
    return f"{label:<10}  {bar:<22}  {value:<14}  {note}".rstrip()


def _fmt_window(window_seconds: int) -> str:
    if window_seconds % 3600 == 0:
        return f"{window_seconds // 3600}h"
    elif window_seconds % 60 == 0:
        return f"{window_seconds // 60}m"
    return f"{window_seconds}s"


def _fmt_distribution_row(name: str, dist: dict[str, Any]) -> str:
    """One reservoir summary line with full metric name and second-unit percentiles."""
    samples = dist.get("samples") or []
    kept = len(samples) if isinstance(samples, list) else 0
    seen = dist.get("count", 0)

    def _p(key: str) -> str:
        value = dist.get(key)
        if value is None:
            return "-"
        try:
            return f"{float(value):.2f}"
        except (TypeError, ValueError):
            return "-"

    return f"{name:<16} {_fmt_int(seen):>8} {_fmt_int(kept):>6} " f"{_p('p50'):>8} {_p('p95'):>8} {_p('p99'):>8}"


def _fmt_counter_group(values: dict[str, int], fields: tuple[tuple[str, str], ...]) -> str:
    return "  ".join(f"{label}={_fmt_int(values.get(name, 0))}" for name, label in fields)


_FLOW_COUNTERS = (
    ("enqueued", "enq"),
    ("candidates", "cand"),
    ("reserved", "res"),
    ("dispatched", "sent"),
    ("completed", "done"),
)
_ISSUE_COUNTERS = (
    ("enqueue_duplicate", "dup"),
    ("enqueue_capacity_rejected", "cap_reject"),
    ("enqueue_producer_rejected", "prod_reject"),
    ("enqueue_unavailable", "unavail"),
    ("blocked", "blocked"),
    ("congestion", "congest"),
    ("missing", "missing"),
    ("publish_failed", "pub_fail"),
    ("celery_failure", "celery_fail"),
    ("pump_missed", "missed"),
    ("pump_lock_skip", "lock_skip"),
)
_TICK_ISSUE_COUNTERS = (
    ("blocked", "blocked"),
    ("congestion", "congest"),
    ("missing", "missing"),
    ("publish_failed", "pub_fail"),
    ("pump_missed", "missed"),
    ("pump_lock_skip", "lock_skip"),
)


def _fmt_decide_note(decide_tick_id: int, last_tick_id: int) -> str:
    """Show absolute decide/last ticks plus signed lag vs expected last tick.

    ``Δ+1``: pump already decided in the current interval.
    ``Δ0``: decide matches the expected previous slot.
    ``Δ-N``: decide is N pump intervals behind.
    """
    if decide_tick_id < 0:
        return "decide=?"
    try:
        last = int(last_tick_id)
    except (TypeError, ValueError):
        return f"decide=#{decide_tick_id}"
    delta = decide_tick_id - last
    sign = f"+{delta}" if delta > 0 else str(delta)
    return f"decide=#{decide_tick_id} (last=#{last}, Δ{sign} tick)"


def _decide_tick_delta(controller: dict[str, Any], last_tick_id: int) -> Optional[int]:
    try:
        decide_tick_id = int(controller.get("tick_id", ""))
    except (TypeError, ValueError):
        return None
    if decide_tick_id < 0:
        return None
    try:
        return decide_tick_id - int(last_tick_id)
    except (TypeError, ValueError):
        return None


@dataclass
class TaskOutcomeStats:
    task_key: str
    outcomes: dict[str, int] = field(default_factory=dict)


@dataclass
class QueueDispatchReport:
    namespace: str
    timestamp: float
    window_seconds: int
    tick_seconds: int
    pending_total: int
    pending_ready: int
    pending_delaying: int
    inflight: int
    config: dict[str, Any]
    controller: dict[str, Any]
    counters: dict[str, int]
    distributions: dict[str, dict[str, Any]]
    pump_lock: dict[str, Any] = field(default_factory=dict)
    producer_lock: dict[str, Any] = field(default_factory=dict)
    diagnosis: list[str] = field(default_factory=list)
    partial: bool = False
    last_tick_id: int = 0
    last_tick_counts: dict[str, int] = field(default_factory=dict)

    def format_summary(self) -> str:
        limits = (
            f"admitted={self.config.get('max_admitted_jobs', '?')} " f"inflight={self.config.get('max_inflight', '?')}"
        )
        budget = self.controller.get("effective_budget", "?")
        cwnd = self.controller.get("congestion_window", "?")
        flow = self.controller.get("flow_window", "?")
        pump_state = str(self.pump_lock.get("state") or "unknown")
        producer_state = str(self.producer_lock.get("state") or "unknown")
        lines = [
            (
                f"dispatch queue[{self.namespace}] @ {_fmt_epoch(self.timestamp)} "
                f"partial={self.partial} pump={pump_state} producer={producer_state}"
            ),
            f"  pending={self.pending_total} ready={self.pending_ready} delaying={self.pending_delaying}",
            f"  inflight={self.inflight} budget={budget} flow={flow} cwnd={cwnd} {limits}",
        ]
        if self.last_tick_counts:
            tick_bits = ", ".join(f"{k}={v}" for k, v in sorted(self.last_tick_counts.items()))
            lines.append(f"  last_tick={_fmt_tick(self.last_tick_id)} (#{self.last_tick_id}): {tick_bits}")
        if self.counters:
            lines.append("  counters: " + ", ".join(f"{k}={v}" for k, v in sorted(self.counters.items())))
        if self.diagnosis:
            lines.append("  diagnosis: " + ", ".join(self.diagnosis))
        return "\n".join(lines)

    def format_dashboard(self) -> str:
        """Compact fixed-order view of live, tick, rolling, and latency state."""
        max_inflight = int(self.config.get("max_inflight", 0) or 0)
        max_admitted = int(self.config.get("max_admitted_jobs", 0) or 0)

        budget = int(self.controller.get("effective_budget", 0) or 0)
        raw_flow = self.controller.get("flow_window")
        if raw_flow is None or raw_flow == "":
            flow_window = max(0, max_inflight - max(0, int(self.inflight)))
        else:
            try:
                flow_window = int(raw_flow)
            except (TypeError, ValueError):
                flow_window = 0
        try:
            congestion_window = int(self.controller.get("congestion_window") or 0)
        except (TypeError, ValueError):
            congestion_window = 0
        aimd = str(self.controller.get("aimd_action", "?") or "?")
        pump_state = str(self.pump_lock.get("state") or "unknown")
        pump_ttl = self.pump_lock.get("ttl_seconds")
        pump_label = pump_state
        if pump_state == "paused":
            if pump_ttl == -1:
                pump_label += "(until_resume)"
            elif pump_ttl is not None:
                pump_label += f"({_fmt_int(pump_ttl)}s)"
        producer_state = str(self.producer_lock.get("state") or "unknown")
        producer_ttl = self.producer_lock.get("ttl_seconds")
        producer_label = producer_state
        if producer_state == "paused":
            if producer_ttl == -1:
                producer_label += "(until_resume)"
            elif producer_ttl is not None:
                producer_label += f"({_fmt_int(producer_ttl)}s)"
        decide_tick = self.controller.get("tick_id", "")
        try:
            decide_tick_id = int(decide_tick)
        except (TypeError, ValueError):
            decide_tick_id = -1

        live_slots = max(0, max_inflight - max(0, int(self.inflight))) if max_inflight else 0
        outstanding = max(0, int(self.pending_total)) + max(0, int(self.inflight))
        admitted_cap = max_admitted or max(outstanding, 1)
        inflight_cap = max_inflight or max(self.inflight, 1)

        decide_note = _fmt_decide_note(decide_tick_id, self.last_tick_id)

        issues = [item for item in self.diagnosis if item != "healthy"]
        status = (
            "PAUSED"
            if pump_state == "paused" or producer_state == "paused"
            else ("PARTIAL" if self.partial else ("WARN" if issues else "HEALTHY"))
        )
        header = (
            f"dispatch[{self.namespace}]  {_fmt_epoch(self.timestamp)}  "
            f"status={status}  pump={pump_label}  producer={producer_label}  window={_fmt_window(self.window_seconds)}"
        )
        rule = "-" * max(100, len(header))
        lines = [
            header,
            rule,
            _dash_row(
                "queue",
                _progress_bar(outstanding, admitted_cap),
                f"{_fmt_int(outstanding)}/{_fmt_int(max_admitted or '?')}",
                (
                    f"pending={_fmt_int(self.pending_total)}  ready={_fmt_int(self.pending_ready)}  "
                    f"delay={_fmt_int(self.pending_delaying)}"
                ),
            ),
            _dash_row(
                "inflight",
                _progress_bar(self.inflight, inflight_cap),
                f"{_fmt_int(self.inflight)}/{_fmt_int(max_inflight or '?')}",
                f"free={_fmt_int(live_slots)}",
            ),
            _dash_row(
                "control",
                "",
                f"budget={_fmt_int(budget)}",
                (f"flow={_fmt_int(flow_window)}  cwnd={_fmt_int(congestion_window)}  " f"aimd={aimd}  {decide_note}"),
            ),
            rule,
            (
                f"tick #{self.last_tick_id}  {_fmt_tick(self.last_tick_id)}  "
                f"{_fmt_counter_group(self.last_tick_counts, _FLOW_COUNTERS[1:])}"
            ),
            ("tick issues  " + _fmt_counter_group(self.last_tick_counts, _TICK_ISSUE_COUNTERS)),
            rule,
            f"window flow    {_fmt_counter_group(self.counters, _FLOW_COUNTERS)}",
            f"window issues  {_fmt_counter_group(self.counters, _ISSUE_COUNTERS)}",
            rule,
            f"{'latency(s)':<16} {'sampled':>8} {'kept':>6} {'p50':>8} {'p95':>8} {'p99':>8}",
        ]
        for name in DISTRIBUTION_NAMES:
            dist = self.distributions.get(name) or {}
            compact_name = name.removesuffix("_seconds")
            row = _fmt_distribution_row(compact_name, dist)
            lines.append(row)
        if issues:
            lines.extend((rule, f"diagnosis  {', '.join(issues)}"))
        return "\n".join(lines)


@dataclass
class TaskDispatchReport:
    task_key: str
    timestamp: float
    window_seconds: int
    pending: int
    inflight: int
    backlog: int
    counters: dict[str, int]
    outcomes: dict[str, int]
    partial: bool = False

    def format_summary(self) -> str:
        values = ", ".join(f"{key}={value}" for key, value in sorted(self.counters.items())) or "no metrics"
        return (
            f"dispatch task[{self.task_key}] window={self.window_seconds}s partial={self.partial}\n"
            f"  pending={self.pending} inflight={self.inflight} backlog={self.backlog}\n"
            f"  {values}"
        )


@dataclass
class DispatchStatsSnapshot:
    timestamp: float
    tick_seconds: int
    pending_total: int
    pending_ready: int
    pending_delaying: int
    inflight: int
    registered: dict[str, Any]
    pump_config: dict[str, Any]
    queues: list[QueueDispatchReport]
    outcomes_by_task: list[TaskOutcomeStats]

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "tick_seconds": self.tick_seconds,
            "pending_total": self.pending_total,
            "pending_ready": self.pending_ready,
            "pending_delaying": self.pending_delaying,
            "inflight": self.inflight,
            "registered": self.registered,
            "pump_config": self.pump_config,
            "queues": [asdict(item) for item in self.queues],
            "outcomes_by_task": [asdict(item) for item in self.outcomes_by_task],
        }

    def format_summary(self) -> str:
        lines = [
            f"dispatch stats @ {_fmt_epoch(self.timestamp)} tick={self.tick_seconds}s",
            f"  pending: total={self.pending_total} ready={self.pending_ready} delaying={self.pending_delaying}",
            f"  inflight={self.inflight}",
            f"  registered_tasks={len(self.registered)}",
        ]
        lines.extend(report.format_summary() for report in self.queues)
        return "\n".join(lines)

    def format_dashboard(self) -> str:
        lines = [
            f"dispatch stats @ {_fmt_epoch(self.timestamp)} tick={self.tick_seconds}s",
            (
                f"pending total={self.pending_total} ready={self.pending_ready} "
                f"delaying={self.pending_delaying}  inflight={self.inflight}"
            ),
            "",
        ]
        for report in self.queues:
            lines.append(report.format_dashboard())
            lines.append("")
        return "\n".join(lines).rstrip()


class DispatchStats:
    """Read bounded dispatch metrics and live queue state without key scans."""

    @staticmethod
    def _window(window_seconds: int) -> int:
        return max(PUMP_INTERVAL_SECONDS, min(int(window_seconds), MAX_REPORT_WINDOW_SECONDS))

    @classmethod
    def queue_report(
        cls,
        namespace: str,
        *,
        window_seconds: int = DEFAULT_REPORT_WINDOW_SECONDS,
    ) -> QueueDispatchReport:
        now = time.time()
        window_seconds = cls._window(window_seconds)
        queue_cls = DispatchQueue.queue_for_namespace(namespace)
        if queue_cls is None:
            return QueueDispatchReport(
                namespace=namespace,
                timestamp=now,
                window_seconds=window_seconds,
                tick_seconds=PUMP_INTERVAL_SECONDS,
                pending_total=-1,
                pending_ready=-1,
                pending_delaying=-1,
                inflight=-1,
                config={},
                controller={},
                counters={},
                distributions={},
                diagnosis=["unregistered_queue"],
                partial=True,
            )

        partial = False
        try:
            config_obj = queue_cls.load_config()
            config = {
                "max_admitted_jobs": config_obj.max_admitted_jobs,
                "max_inflight": config_obj.max_inflight,
            }
        except Exception:
            config = {}
            partial = True
        counters = DispatchMetrics.aggregate_queue_counters(
            namespace,
            start_at=now - window_seconds,
            end_at=now,
        )
        distributions = {
            name: asdict(
                DispatchMetrics.distribution(
                    namespace,
                    name,
                    start_at=now - window_seconds,
                    end_at=now,
                )
            )
            for name in DISTRIBUTION_NAMES
        }
        controller = PumpController.read_state(namespace)
        if not controller:
            partial = True
        # Deferred import: importing pump registers a periodic task (DB write).
        from backend.db_periodic_task.dispatch.pump import inspect_queue_pump_lock

        pump_lock_raw = inspect_queue_pump_lock(namespace)
        pump_lock = pump_lock_raw if isinstance(pump_lock_raw, dict) else {}
        # Deferred import: producer.py only pulls in lua + routing.
        from backend.db_periodic_task.dispatch.producer import inspect_queue_producer_lock

        producer_lock_raw = inspect_queue_producer_lock(namespace)
        producer_lock = producer_lock_raw if isinstance(producer_lock_raw, dict) else {}
        observed_tick = tick_id(now)
        last_tick_id = observed_tick - 1
        try:
            last_tick_counts = DispatchMetrics.queue_tick_counts(namespace, last_tick_id)
        except Exception:
            last_tick_counts = {}
            partial = True
        report = QueueDispatchReport(
            namespace=namespace,
            timestamp=now,
            window_seconds=window_seconds,
            tick_seconds=PUMP_INTERVAL_SECONDS,
            pending_total=queue_cls.pending_count(),
            pending_ready=queue_cls.ready_count(now),
            pending_delaying=queue_cls.delaying_count(now),
            inflight=queue_cls.inflight_count(),
            config=config,
            controller=controller,
            counters=counters,
            distributions=distributions,
            pump_lock=pump_lock,
            producer_lock=producer_lock,
            partial=partial,
            last_tick_id=last_tick_id,
            last_tick_counts=last_tick_counts,
        )
        report.diagnosis = cls._diagnose(report)
        return report

    @classmethod
    def task_report(
        cls,
        task_key: str,
        *,
        window_seconds: int = DEFAULT_REPORT_WINDOW_SECONDS,
    ) -> TaskDispatchReport:
        now = time.time()
        window_seconds = cls._window(window_seconds)
        registered = cls._load_registered()
        metadata = registered.get(task_key)
        namespace = metadata.get("namespace", "") if isinstance(metadata, dict) else ""
        queue_cls = DispatchQueue.queue_for_namespace(namespace) if namespace else None
        pending, inflight = queue_cls.task_counts(task_key) if queue_cls else (-1, -1)
        backlog = pending + inflight if pending >= 0 and inflight >= 0 else -1
        counters = DispatchMetrics.aggregate_task_counters(
            namespace,
            task_key,
            start_at=now - window_seconds,
            end_at=now,
        )
        outcomes = {
            name.removeprefix("outcome:"): count for name, count in counters.items() if name.startswith("outcome:")
        }
        return TaskDispatchReport(
            task_key=task_key,
            timestamp=now,
            window_seconds=window_seconds,
            pending=pending,
            inflight=inflight,
            backlog=backlog,
            counters=counters,
            outcomes=outcomes,
            partial=metadata is None or backlog < 0,
        )

    @classmethod
    def diagnose_queue(cls, namespace: str) -> list[str]:
        return cls.queue_report(namespace).diagnosis

    @classmethod
    def watch_queue(
        cls,
        namespace: str,
        *,
        interval_seconds: Optional[float] = None,
        ticks: Optional[int] = None,
        window_seconds: int = DEFAULT_REPORT_WINDOW_SECONDS,
        clear: bool = True,
        stream: Optional[TextIO] = None,
    ) -> None:
        """Live-refresh an ASCII dashboard for one queue (Ctrl-C to stop).

        ``namespace`` selects which registered queue to watch (e.g. ``"ai"``).
        Default refresh is 2s so the NOW (live capacity) layer stays responsive
        between pumps. Pass ``interval_seconds=PUMP_INTERVAL_SECONDS`` (10) if you
        prefer one frame per pump tick for LAST TICK alignment.
        """
        if not namespace:
            raise ValueError("namespace is required")
        out = stream or sys.stdout
        interval = float(2.0 if interval_seconds is None else interval_seconds)
        if interval < 0:
            raise ValueError("interval_seconds must be >= 0")
        use_clear = bool(clear and hasattr(out, "isatty") and out.isatty())
        n = 0
        try:
            while ticks is None or n < ticks:
                report = cls.queue_report(namespace, window_seconds=window_seconds)
                frame = report.format_dashboard()
                if use_clear:
                    out.write(_ANSI_CLEAR)
                elif n:
                    out.write("\n" + "=" * 72 + "\n")
                out.write(frame + "\n")
                out.flush()
                n += 1
                if ticks is not None and n >= ticks:
                    break
                if interval > 0:
                    time.sleep(interval)
        except KeyboardInterrupt:
            out.write("\n")
            out.flush()

    @staticmethod
    def _diagnose(report: QueueDispatchReport) -> list[str]:
        diagnosis: list[str] = []
        pump_state = str(report.pump_lock.get("state") or "")
        if pump_state == "paused":
            diagnosis.append("pump_paused")
        else:
            delta = _decide_tick_delta(report.controller, report.last_tick_id)
            if delta is not None and delta < 0:
                diagnosis.append("pump_delayed")
        producer_state = str(report.producer_lock.get("state") or "")
        if producer_state == "paused":
            diagnosis.append("producer_paused")
        max_inflight = int(report.config.get("max_inflight", 0) or 0)
        if max_inflight and report.inflight >= max_inflight:
            diagnosis.append("inflight_saturated")
        if report.counters.get("publish_failed", 0):
            diagnosis.append("broker_publish_failures")
        if report.counters.get("blocked", 0):
            diagnosis.append("reservation_blocked")
        if report.counters.get("pump_missed", 0):
            diagnosis.append("pump_missed")
        if report.counters.get("pump_lock_skip", 0):
            diagnosis.append("pump_lock_skip")
        if report.partial:
            diagnosis.append("metrics_partial")
        return diagnosis or ["healthy"]

    @classmethod
    def snapshot(
        cls,
        *,
        include_outcomes: bool = True,
        window_seconds: int = DEFAULT_REPORT_WINDOW_SECONDS,
    ) -> DispatchStatsSnapshot:
        now = time.time()
        window_seconds = cls._window(window_seconds)
        registered = cls._load_registered()
        queues = [
            cls.queue_report(queue_cls.namespace, window_seconds=window_seconds)
            for queue_cls in DispatchQueue.iter_queues()
        ]
        outcomes = []
        if include_outcomes:
            for task_key, metadata in registered.items():
                namespace = metadata.get("namespace", "") if isinstance(metadata, dict) else ""
                counters = DispatchMetrics.aggregate_task_counters(
                    namespace,
                    task_key,
                    start_at=now - window_seconds,
                    end_at=now,
                )
                outcomes.append(
                    TaskOutcomeStats(
                        task_key=task_key,
                        outcomes={
                            name.removeprefix("outcome:"): count
                            for name, count in counters.items()
                            if name.startswith("outcome:")
                        },
                    )
                )
        return DispatchStatsSnapshot(
            timestamp=now,
            tick_seconds=PUMP_INTERVAL_SECONDS,
            pending_total=DispatchQueue.aggregate_pending_count(),
            pending_ready=DispatchQueue.aggregate_ready_count(now),
            pending_delaying=DispatchQueue.aggregate_delaying_count(now),
            inflight=DispatchQueue.aggregate_inflight_count(),
            registered=registered,
            pump_config=cls._load_pump_config(),
            queues=queues,
            outcomes_by_task=outcomes,
        )

    @staticmethod
    def _load_registered() -> dict[str, Any]:
        try:
            raw = routing.global_conn().hgetall(KEY_REGISTERED) or {}
            result = {}
            for key, value in raw.items():
                key = key.decode() if isinstance(key, bytes) else key
                value = value.decode() if isinstance(value, bytes) else value
                result[key] = json.loads(value) if isinstance(value, str) else value
            return result
        except Exception:
            return {}

    @staticmethod
    def _load_pump_config() -> dict[str, int]:
        try:
            config = DispatchPumpConfig()
            return {"max_parallel_queues": config.max_parallel_queues}
        except Exception:
            return {}

    @classmethod
    def parse_raw(cls, raw: dict) -> DispatchStatsSnapshot:
        queues = [QueueDispatchReport(**item) for item in raw.get("queues", [])]
        outcomes = [
            TaskOutcomeStats(task_key=item["task_key"], outcomes=item.get("outcomes", {}))
            for item in raw.get("outcomes_by_task", [])
        ]
        return DispatchStatsSnapshot(
            timestamp=float(raw.get("timestamp", 0)),
            tick_seconds=int(raw.get("tick_seconds", PUMP_INTERVAL_SECONDS)),
            pending_total=int(raw.get("pending_total", -1)),
            pending_ready=int(raw.get("pending_ready", -1)),
            pending_delaying=int(raw.get("pending_delaying", -1)),
            inflight=int(raw.get("inflight", -1)),
            registered=raw.get("registered", {}),
            pump_config=raw.get("pump_config", {}),
            queues=queues,
            outcomes_by_task=outcomes,
        )

import time
from unittest.mock import MagicMock, patch

import pytest

from backend.db_periodic_task.dispatch.config import DispatchQueueConfig
from backend.db_periodic_task.dispatch.controller import PumpControlDecision, PumpController
from backend.db_periodic_task.dispatch.metrics import (
    METRICS_RETENTION_SECONDS,
    RESERVOIR_SIZE,
    DispatchMetrics,
    DistributionSummary,
)
from backend.db_periodic_task.dispatch.observability import DispatchStats, QueueDispatchReport


class TestDispatchMetrics:
    def test_queue_counter_uses_tick_field_and_retention(self):
        script = MagicMock()
        with patch.object(DispatchMetrics, "_get_counter_script", return_value=script):
            DispatchMetrics.record_queue_counter("ai", "dispatched", 3, timestamp=120.0)

        assert script.call_args.kwargs["keys"] == ["dispatch:metrics:queue:ai:0"]
        assert script.call_args.kwargs["args"] == ["t:12:dispatched", 3, METRICS_RETENTION_SECONDS]

    def test_reservoir_is_hourly_and_bounded(self):
        script = MagicMock()
        with patch.object(DispatchMetrics, "_get_reservoir_script", return_value=script):
            DispatchMetrics.record_sample("ai", "execution_seconds", 1.25, timestamp=3601.0)

        assert script.call_args.kwargs["keys"] == ["dispatch:metrics:sample:ai:1"]
        assert script.call_args.kwargs["args"][0:3] == ["execution_seconds", "3601.0:1.25", RESERVOIR_SIZE]

    def test_distribution_sampling_is_stable_and_not_universal(self):
        decisions = [DispatchMetrics.should_sample(f"job:{index}:1.0") for index in range(100)]

        assert decisions == [DispatchMetrics.should_sample(f"job:{index}:1.0") for index in range(100)]
        assert any(decisions)
        assert not all(decisions)

    def test_unsampled_record_does_not_issue_reservoir_eval(self):
        script = MagicMock()
        with patch.object(DispatchMetrics, "should_sample", return_value=False), patch.object(
            DispatchMetrics,
            "_get_reservoir_script",
            return_value=script,
        ):
            DispatchMetrics.record_sample(
                "ai",
                "execution_seconds",
                1.25,
                sample_identity="job:1:1.0",
            )

        script.assert_not_called()

    def test_queue_wait_samples_use_one_bounded_reservoir_eval(self):
        script = MagicMock()
        with patch.object(
            DispatchMetrics,
            "should_sample",
            side_effect=lambda identity: identity.startswith("keep"),
        ), patch.object(
            DispatchMetrics,
            "_get_reservoir_batch_script",
            return_value=script,
        ):
            DispatchMetrics.record_samples(
                "ai",
                "queue_wait_seconds",
                [("keep:1", 1.0), ("drop:1", 2.0), ("keep:2", 3.0)],
                timestamp=3601.0,
            )

        script.assert_called_once()
        args = script.call_args.kwargs["args"]
        assert args[0:4] == ["queue_wait_seconds", RESERVOIR_SIZE, METRICS_RETENTION_SECONDS, 2]
        assert args[4] == "3601.0:1.0"
        assert args[6] == "3601.0:3.0"

    def test_aggregate_task_counters_returns_requested_window(self):
        raw = {
            "m:0:outcome:success": "2",
            "m:1:outcome:error": "1",
            "m:59:outcome:success": "9",
        }
        with patch("backend.db_periodic_task.dispatch.metrics.RedisConn.hgetall", return_value=raw):
            counters = DispatchMetrics.aggregate_task_counters(
                "task",
                start_at=0,
                end_at=120,
            )

        assert counters == {"outcome:success": 2, "outcome:error": 1}

    def test_distribution_filters_partial_hour_samples(self):
        raw = {
            "r:execution_seconds:seen": "100",
            "r:execution_seconds:s:1": "10.0:1.0",
            "r:execution_seconds:s:2": "1000.0:9.0",
        }
        with patch("backend.db_periodic_task.dispatch.metrics.RedisConn.hgetall", return_value=raw):
            summary = DispatchMetrics.distribution(
                "ai",
                "execution_seconds",
                start_at=0,
                end_at=100,
            )

        assert summary.samples == [1.0]
        assert summary.count == 50

    @pytest.mark.parametrize(
        ("percentile", "expected"),
        [(0.50, 2.0), (0.95, 4.0), (0.99, 4.0)],
    )
    def test_percentiles(self, percentile, expected):
        assert DispatchMetrics._percentile([1.0, 2.0, 3.0, 4.0], percentile) == expected

    def test_queue_tick_counts_hmgets_requested_tick_fields(self):
        """Point-read one tick via HMGET; neighbor slots must not leak."""

        def hmget(_key, fields):
            data = {
                "t:12:candidates": "15",
                "t:12:dispatched": "5",
                "t:12:blocked": "1",
                # Neighbor slots — must never be requested for tick 12.
                "t:1:candidates": "99",
                "t:120:candidates": "7",
                "t:13:dispatched": "3",
            }
            return [data.get(field) for field in fields]

        with patch("backend.db_periodic_task.dispatch.metrics.RedisConn.hmget", side_effect=hmget) as mocked:
            # tick_id 12 → timestamp 120 → slot 12
            counts = DispatchMetrics.queue_tick_counts(
                "ai",
                12,
                names=("candidates", "dispatched", "blocked", "missing"),
            )

        assert counts == {"candidates": 15, "dispatched": 5, "blocked": 1}
        assert mocked.call_args.args[1] == [
            "t:12:candidates",
            "t:12:dispatched",
            "t:12:blocked",
            "t:12:missing",
        ]


class TestPumpController:
    @staticmethod
    def _queue(*, inflight: int):
        queue = MagicMock()
        queue.namespace = "ai"
        queue.inflight_count.return_value = inflight
        return queue

    @staticmethod
    def _decide(queue, settings, *, state, previous, tick_id=123):
        with patch(
            "backend.db_periodic_task.dispatch.controller.RedisConn.hgetall",
            return_value=state,
        ), patch.object(DispatchMetrics, "queue_tick_counts", return_value=previous,), patch.object(
            PumpController, "_persist"
        ):
            return PumpController.decide(queue, settings, current_tick_id=tick_id)

    def test_cold_start_uses_ten_percent_of_configured_concurrency(self):
        queue = self._queue(inflight=25)
        settings = DispatchQueueConfig(max_inflight=200)
        decision = self._decide(queue, settings, state={}, previous={})

        assert decision.cold_start is True
        assert decision.aimd_action == "cold_start"
        assert decision.congestion_window == 20
        assert decision.flow_window == 175
        assert decision.effective_budget == 20
        assert decision.available_slots == 175

    def test_warm_decrease_multi_tick_halves_geometrically_then_recovers(self):
        queue = self._queue(inflight=0)
        settings = DispatchQueueConfig(max_inflight=200)
        state = {"tick_id": "122", "effective_budget": "200", "congestion_window": "200"}

        # Sustained congestion halves cwnd every tick: 200 -> 100 -> 50.
        for tick, expected_cwnd in ((123, 100), (124, 50)):
            previous = {"candidates": 500, "dispatched": int(state["effective_budget"]), "congestion": 1}
            decision = self._decide(queue, settings, state=state, previous=previous, tick_id=tick)
            assert decision.aimd_action == "decrease"
            assert decision.congestion_window == expected_cwnd
            assert decision.effective_budget == expected_cwnd
            assert decision.previous_congestion == 1
            state = {
                "tick_id": str(tick),
                "effective_budget": str(decision.effective_budget),
                "congestion_window": str(decision.congestion_window),
            }

        # Congestion clears with the reduced window still saturated: additive
        # increase resumes from 50 by one step (max_inflight // 20 = 10).
        previous = {"candidates": 500, "dispatched": 50}
        decision = self._decide(queue, settings, state=state, previous=previous, tick_id=125)
        assert decision.aimd_action == "increase"
        assert decision.congestion_window == 60

    def test_warm_decrease_floors_at_one(self):
        queue = self._queue(inflight=0)
        settings = DispatchQueueConfig(max_inflight=200)
        state = {"tick_id": "122", "effective_budget": "1", "congestion_window": "1"}
        previous = {"candidates": 10, "dispatched": 1, "congestion": 1}
        decision = self._decide(queue, settings, state=state, previous=previous)

        assert decision.aimd_action == "decrease"
        assert decision.congestion_window == 1

    def test_blocked_and_publish_failed_do_not_trigger_md(self):
        queue = self._queue(inflight=0)
        settings = DispatchQueueConfig(max_inflight=200)
        state = {"tick_id": "122", "effective_budget": "50", "congestion_window": "50"}
        previous = {"candidates": 100, "dispatched": 50, "blocked": 3, "publish_failed": 1}
        decision = self._decide(queue, settings, state=state, previous=previous)

        assert decision.aimd_action == "increase"
        assert decision.congestion_window == 60
        assert decision.effective_budget == 60
        assert decision.previous_blocked == 3
        assert decision.previous_publish_failed == 1

    def test_warm_hold_when_flow_window_limited_previous_tick(self):
        queue = self._queue(inflight=0)
        settings = DispatchQueueConfig(max_inflight=200)
        # previous effective budget was clamped by flow window, not cwnd
        state = {"tick_id": "122", "effective_budget": "20", "congestion_window": "50"}
        previous = {"candidates": 100, "dispatched": 20}
        decision = self._decide(queue, settings, state=state, previous=previous)

        assert decision.aimd_action == "hold"
        assert decision.congestion_window == 50
        assert decision.effective_budget == 50

    def test_temporary_slot_shrink_does_not_poison_cwnd(self):
        queue = self._queue(inflight=190)
        settings = DispatchQueueConfig(max_inflight=200)
        state = {"tick_id": "122", "effective_budget": "50", "congestion_window": "50"}
        previous = {"candidates": 100, "dispatched": 50}
        decision = self._decide(queue, settings, state=state, previous=previous)

        assert decision.flow_window == 10
        assert decision.congestion_window == 60
        assert decision.effective_budget == 10

    def test_warm_hold_when_demand_not_saturated(self):
        queue = self._queue(inflight=0)
        settings = DispatchQueueConfig(max_inflight=200)
        state = {"tick_id": "122", "effective_budget": "50", "congestion_window": "50"}
        previous = {"candidates": 20, "dispatched": 20}
        decision = self._decide(queue, settings, state=state, previous=previous)

        assert decision.aimd_action == "hold"
        assert decision.effective_budget == 50
        assert decision.congestion_window == 50

    def test_concurrency_is_always_a_hard_ceiling(self):
        queue = self._queue(inflight=198)
        settings = DispatchQueueConfig(max_inflight=200)
        decision = self._decide(queue, settings, state={}, previous={})

        assert decision.flow_window == 2
        assert decision.congestion_window == 20
        assert decision.effective_budget == 2
        assert decision.effective_budget <= decision.available_slots

    def test_congestion_window_does_not_grow_past_concurrency(self):
        queue = self._queue(inflight=0)
        settings = DispatchQueueConfig(max_inflight=200)
        state = {"tick_id": "122", "effective_budget": "195", "congestion_window": "195"}
        previous = {"candidates": 200, "dispatched": 195}
        decision = self._decide(queue, settings, state=state, previous=previous)

        assert decision.aimd_action == "increase"
        assert decision.congestion_window == 200
        assert decision.effective_budget == 200

    def test_metrics_failure_falls_back_to_aimd_cold_start(self):
        queue = self._queue(inflight=0)
        settings = DispatchQueueConfig(max_inflight=200)
        with patch(
            "backend.db_periodic_task.dispatch.controller.RedisConn.hgetall",
            side_effect=RuntimeError("redis down"),
        ), patch.object(PumpController, "_persist"):
            decision = PumpController.decide(queue, settings, current_tick_id=123)

        assert decision.congestion_window == 20
        assert decision.effective_budget == 20
        assert decision.aimd_action == "cold_start"
        assert decision.partial is True

    def test_controller_state_serializes_booleans_and_reads_typed_values(self):
        pipeline = MagicMock()
        decision = PumpControlDecision(
            namespace="ai",
            tick_id=123,
            effective_budget=50,
            flow_window=100,
            congestion_window=50,
            available_slots=150,
            previous_congestion=1,
            previous_blocked=0,
            previous_publish_failed=0,
            aimd_action="decrease",
            cold_start=True,
            partial=False,
        )
        with patch(
            "backend.db_periodic_task.dispatch.controller.RedisConn.pipeline",
            return_value=pipeline,
        ):
            PumpController._persist(decision)

        pipeline.hset.assert_called_once()
        mapping = pipeline.hset.call_args.kwargs["mapping"]
        assert mapping["cold_start"] == 1
        assert mapping["partial"] == 0
        assert all(not isinstance(value, bool) for value in mapping.values())
        pipeline.expire.assert_called_once()

        raw = {
            "tick_id": "123",
            "effective_budget": "50",
            "flow_window": "100",
            "congestion_window": "50",
            "previous_congestion": "1",
            "aimd_action": "decrease",
            "cold_start": "1",
            "partial": "0",
            "updated_at": "1710000000.5",
        }
        with patch(
            "backend.db_periodic_task.dispatch.controller.RedisConn.hgetall",
            return_value=raw,
        ):
            state = PumpController.read_state("ai")

        assert state["tick_id"] == 123
        assert state["congestion_window"] == 50
        assert state["previous_congestion"] == 1
        assert state["aimd_action"] == "decrease"
        assert state["cold_start"] is True
        assert state["partial"] is False
        assert state["updated_at"] == 1710000000.5


class TestDispatchReports:
    def test_queue_report_explains_concurrency_ceiling(self):
        queue = MagicMock()
        queue.load_config.return_value = DispatchQueueConfig(max_inflight=200)
        queue.pending_count.return_value = 20
        queue.ready_count.return_value = 20
        queue.delaying_count.return_value = 0
        queue.inflight_count.return_value = 10
        with patch(
            "backend.db_periodic_task.dispatch.observability.DispatchQueue.queue_for_namespace",
            return_value=queue,
        ), patch(
            "backend.db_periodic_task.dispatch.pump.inspect_queue_pump_lock",
            return_value={"state": "free"},
        ), patch.object(
            DispatchMetrics,
            "aggregate_queue_counters",
            return_value={"dispatched": 100},
        ), patch.object(
            DispatchMetrics,
            "distribution",
            return_value=DistributionSummary(),
        ), patch.object(
            DispatchMetrics,
            "queue_tick_counts",
            return_value={"candidates": 5, "dispatched": 5},
        ), patch.object(
            PumpController,
            "read_state",
            return_value={"effective_budget": "100", "flow_window": "190"},
        ):
            report = DispatchStats.queue_report("ai")

        assert report.partial is False
        assert report.tick_seconds == 10
        assert report.last_tick_counts == {"candidates": 5, "dispatched": 5}
        assert report.diagnosis == ["healthy"]
        assert "budget=100" in report.format_summary()
        assert "last_tick=" in report.format_summary()

    def test_queue_dashboard_shows_paused_pump(self):
        queue = MagicMock()
        queue.load_config.return_value = DispatchQueueConfig(max_inflight=50)
        queue.pending_count.return_value = 20
        queue.ready_count.return_value = 20
        queue.delaying_count.return_value = 0
        queue.inflight_count.return_value = 0
        with patch(
            "backend.db_periodic_task.dispatch.observability.DispatchQueue.queue_for_namespace",
            return_value=queue,
        ), patch(
            "backend.db_periodic_task.dispatch.pump.inspect_queue_pump_lock",
            return_value={
                "state": "paused",
                "held": True,
                "owner": "dispatch:paused",
                "ttl_seconds": 90,
            },
        ), patch.object(
            DispatchMetrics, "aggregate_queue_counters", return_value={}
        ), patch.object(
            DispatchMetrics,
            "distribution",
            return_value=DistributionSummary(),
        ), patch.object(
            DispatchMetrics,
            "queue_tick_counts",
            return_value={},
        ), patch.object(
            PumpController,
            "read_state",
            return_value={"effective_budget": "5", "flow_window": "50"},
        ):
            report = DispatchStats.queue_report("dummy")

        assert report.pump_lock["state"] == "paused"
        assert report.diagnosis == ["pump_paused"]
        assert "pump=paused" in report.format_summary()
        dashboard = report.format_dashboard()
        assert "status=PAUSED" in dashboard
        assert "pump=paused(90s)" in dashboard
        assert "diagnosis  pump_paused" in dashboard

    def test_queue_dashboard_renders_bars_and_last_tick(self):
        report = QueueDispatchReport(
            namespace="ai",
            timestamp=1710000000.0,
            window_seconds=3600,
            tick_seconds=10,
            pending_total=12,
            pending_ready=10,
            pending_delaying=2,
            inflight=40,
            config={
                "max_inflight": 200,
                "max_admitted_jobs": 2000,
            },
            controller={
                "effective_budget": 15,
                "flow_window": 100,
                "congestion_window": 15,
                "available_slots": 160,
                "aimd_action": "increase",
                "tick_id": "123",
            },
            counters={"dispatched": 100, "completed": 80},
            distributions={
                "queue_wait_seconds": {
                    "count": 40,
                    "samples": [1.0, 2.0, 3.0],
                    "p50": 2.0,
                    "p95": 3.0,
                    "p99": 3.0,
                },
                "execution_seconds": {"count": 0, "samples": [], "p50": None, "p95": None, "p99": None},
                "pump_seconds": {"count": 0, "samples": [], "p50": None, "p95": None, "p99": None},
            },
            diagnosis=["healthy"],
            last_tick_id=123,
            last_tick_counts={"candidates": 20, "dispatched": 15, "blocked": 0, "reserved": 15},
        )
        dashboard = report.format_dashboard()
        assert "dispatch[ai]" in dashboard
        assert "status=HEALTHY" in dashboard
        assert "window=1h" in dashboard
        assert "inflight" in dashboard and "[#" in dashboard
        assert "aimd=increase" in dashboard
        assert "cand=20" in dashboard
        assert "sent=15" in dashboard
        assert "#123" in dashboard
        assert time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(123 * 10)) in dashboard
        assert time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(1710000000.0)) in dashboard
        assert "52/2000" in dashboard  # pending + inflight admission occupancy
        assert "free=" in dashboard
        assert "window flow" in dashboard
        assert "window issues" in dashboard
        assert "queue_wait" in dashboard
        assert "latency(s)" in dashboard
        assert "     2.00" in dashboard  # p50 column

    def test_watch_queue_prints_frames(self):
        from io import StringIO

        frames = [
            QueueDispatchReport(
                namespace="ai",
                timestamp=1.0,
                window_seconds=60,
                tick_seconds=10,
                pending_total=1,
                pending_ready=1,
                pending_delaying=0,
                inflight=0,
                config={"max_inflight": 3},
                controller={"effective_budget": 1, "aimd_action": "hold"},
                counters={},
                distributions={},
                diagnosis=["healthy"],
                last_tick_id=1,
                last_tick_counts={"candidates": 1, "dispatched": 1},
            ),
            QueueDispatchReport(
                namespace="ai",
                timestamp=2.0,
                window_seconds=60,
                tick_seconds=10,
                pending_total=0,
                pending_ready=0,
                pending_delaying=0,
                inflight=1,
                config={"max_inflight": 3},
                controller={"effective_budget": 2, "aimd_action": "increase"},
                counters={},
                distributions={},
                diagnosis=["healthy"],
                last_tick_id=2,
                last_tick_counts={"candidates": 2, "dispatched": 1},
            ),
        ]
        stream = StringIO()
        with patch.object(DispatchStats, "queue_report", side_effect=frames), patch(
            "backend.db_periodic_task.dispatch.observability.time.sleep"
        ):
            DispatchStats.watch_queue("ai", ticks=2, interval_seconds=0, clear=False, stream=stream)

        output = stream.getvalue()
        assert output.count("dispatch[ai]") == 2
        assert "aimd=hold" in output
        assert "aimd=increase" in output

    def test_task_report_aggregates_recent_outcomes(self):
        counters = {"enqueued": 4, "outcome:success": 3, "outcome:error": 1}
        queue_cls = MagicMock()
        queue_cls.task_counts.return_value = (7, 2)
        with patch.object(DispatchMetrics, "aggregate_task_counters", return_value=counters), patch.object(
            DispatchStats,
            "_load_registered",
            return_value={"redis.check": {"namespace": "ai"}},
        ), patch(
            "backend.db_periodic_task.dispatch.observability.DispatchQueue.queue_for_namespace",
            return_value=queue_cls,
        ):
            report = DispatchStats.task_report("redis.check")

        assert report.outcomes == {"success": 3, "error": 1}
        assert report.pending == 7
        assert report.inflight == 2
        assert report.backlog == 9
        assert report.partial is False
        assert "pending=7 inflight=2 backlog=9" in report.format_summary()

    def test_snapshot_reuses_registry_without_loading_task_backlogs(self):
        registered = {"task.a": {"namespace": "ai"}, "task.b": {"namespace": "ai"}}
        with patch.object(DispatchStats, "_load_registered", return_value=registered), patch(
            "backend.db_periodic_task.dispatch.observability.DispatchQueue.iter_queues",
            return_value=[],
        ), patch(
            "backend.db_periodic_task.dispatch.observability.DispatchQueue.aggregate_pending_count",
            return_value=0,
        ), patch(
            "backend.db_periodic_task.dispatch.observability.DispatchQueue.aggregate_ready_count",
            return_value=0,
        ), patch(
            "backend.db_periodic_task.dispatch.observability.DispatchQueue.aggregate_delaying_count",
            return_value=0,
        ), patch(
            "backend.db_periodic_task.dispatch.observability.DispatchQueue.aggregate_inflight_count",
            return_value=0,
        ), patch.object(
            DispatchStats,
            "_load_pump_config",
            return_value={},
        ), patch.object(
            DispatchMetrics,
            "aggregate_task_counters",
            side_effect=[
                {"outcome:success": 2},
                {"outcome:error": 1},
            ],
        ) as aggregate_counters, patch.object(
            DispatchStats, "task_report"
        ) as task_report:
            snapshot = DispatchStats.snapshot()

        assert [(item.task_key, item.outcomes) for item in snapshot.outcomes_by_task] == [
            ("task.a", {"success": 2}),
            ("task.b", {"error": 1}),
        ]
        assert aggregate_counters.call_count == 2
        task_report.assert_not_called()

    def test_task_report_is_partial_when_live_backlog_is_unavailable(self):
        with patch.object(DispatchMetrics, "aggregate_task_counters", return_value={}), patch.object(
            DispatchStats,
            "_load_registered",
            return_value={"redis.check": {"namespace": "missing"}},
        ), patch(
            "backend.db_periodic_task.dispatch.observability.DispatchQueue.queue_for_namespace",
            return_value=None,
        ):
            report = DispatchStats.task_report("redis.check")

        assert report.pending == -1
        assert report.inflight == -1
        assert report.backlog == -1
        assert report.partial is True

    def test_unregistered_queue_returns_partial_report(self):
        with patch(
            "backend.db_periodic_task.dispatch.observability.DispatchQueue.queue_for_namespace",
            return_value=None,
        ):
            report = DispatchStats.queue_report("missing")

        assert report.partial is True
        assert report.diagnosis == ["unregistered_queue"]

    def test_dashboard_shows_relative_decide_tick_delta(self):
        base = dict(
            namespace="dummy",
            timestamp=1710000000.0,
            window_seconds=3600,
            tick_seconds=10,
            pending_total=1,
            pending_ready=1,
            pending_delaying=0,
            inflight=0,
            config={"max_inflight": 50, "max_admitted_jobs": 2000},
            counters={},
            distributions={},
            pump_lock={"state": "free"},
            last_tick_counts={},
        )
        ahead = QueueDispatchReport(
            **base,
            controller={"tick_id": 125, "effective_budget": 1, "aimd_action": "hold"},
            diagnosis=["healthy"],
            last_tick_id=124,
        )
        aligned = QueueDispatchReport(
            **base,
            controller={"tick_id": 124, "effective_budget": 1, "aimd_action": "hold"},
            diagnosis=["healthy"],
            last_tick_id=124,
        )
        behind = QueueDispatchReport(
            **base,
            controller={"tick_id": 122, "effective_budget": 1, "aimd_action": "hold"},
            diagnosis=["pump_delayed"],
            last_tick_id=124,
        )

        assert "decide=#125 (last=#124, Δ+1 tick)" in ahead.format_dashboard()
        assert "decide=#124 (last=#124, Δ0 tick)" in aligned.format_dashboard()
        delayed = behind.format_dashboard()
        assert "decide=#122 (last=#124, Δ-2 tick)" in delayed
        assert "status=WARN" in delayed
        assert "diagnosis  pump_delayed" in delayed

    def test_diagnose_marks_pump_delayed_and_missed_counters(self):
        delayed = QueueDispatchReport(
            namespace="dummy",
            timestamp=1.0,
            window_seconds=60,
            tick_seconds=10,
            pending_total=5,
            pending_ready=5,
            pending_delaying=0,
            inflight=0,
            config={"max_inflight": 50},
            controller={"tick_id": 10},
            counters={"pump_missed": 3, "pump_lock_skip": 1},
            distributions={},
            pump_lock={"state": "free"},
            last_tick_id=12,
        )
        assert DispatchStats._diagnose(delayed) == ["pump_delayed", "pump_missed", "pump_lock_skip"]

        paused = QueueDispatchReport(
            namespace="dummy",
            timestamp=1.0,
            window_seconds=60,
            tick_seconds=10,
            pending_total=5,
            pending_ready=5,
            pending_delaying=0,
            inflight=0,
            config={"max_inflight": 50},
            controller={"tick_id": 10},
            counters={},
            distributions={},
            pump_lock={"state": "paused", "ttl_seconds": -1},
            last_tick_id=12,
        )
        assert DispatchStats._diagnose(paused) == ["pump_paused"]
        assert "missed=" in delayed.format_dashboard()
        assert "lock_skip=" in delayed.format_dashboard()

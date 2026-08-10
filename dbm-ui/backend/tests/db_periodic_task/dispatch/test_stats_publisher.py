import importlib
import json
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from backend.db_periodic_task.dispatch.observability import DispatchStats, QueueDispatchReport, TaskDispatchReport


@pytest.fixture(scope="module")
def sp(django_db_setup, django_db_blocker):
    """Importing the module runs ``@register_periodic_task`` (a DB write), so it
    must happen inside a blocked-DB window, mirroring the pump tests."""
    with django_db_blocker.unblock():
        return importlib.import_module("backend.db_periodic_task.dispatch.stats_publisher")


def _make_queue_report(ns="ai", **overrides):
    base = dict(
        namespace=ns,
        timestamp=time.time(),
        window_seconds=3600,
        tick_seconds=10,
        pending_total=10,
        pending_ready=5,
        pending_delaying=3,
        inflight=2,
        config={"max_admitted_jobs": 100, "max_inflight": 20},
        controller={"effective_budget": 15, "flow_window": 10, "congestion_window": 8},
        counters={"enqueued": 100, "dispatched": 80},
        distributions={
            "queue_wait_seconds": {"p50": 1.2, "p95": 3.4, "p99": 5.6},
            "execution_seconds": {"p50": 2.0, "p95": 4.0, "p99": 8.0},
            "pump_seconds": {},
        },
        pump_lock={"state": "free"},
        producer_lock={"state": "free"},
        diagnosis=["healthy"],
        partial=False,
        last_tick_id=0,
        last_tick_counts={},
    )
    base.update(overrides)
    return QueueDispatchReport(**base)


def _make_task_report(task_key="ai.task", ns="ai", **overrides):
    base = dict(
        task_key=task_key,
        namespace=ns,
        timestamp=time.time(),
        window_seconds=3600,
        pending=2,
        inflight=1,
        backlog=3,
        counters={"outcome:success": 30, "outcome:error": 1},
        outcomes={"success": 30, "error": 1},
        partial=False,
    )
    base.update(overrides)
    return TaskDispatchReport(**base)


def _make_snapshot(queues=None, tasks=None, timestamp=None):
    return SimpleNamespace(
        timestamp=timestamp or time.time(),
        queues=queues or [_make_queue_report()],
        task_reports=tasks or [_make_task_report()],
    )


def _existing_payload(now, age_24h=0):
    gen_24h = now - age_24h
    return {
        "schema_version": 1,
        "generated_at": {"1h": now, "24h": gen_24h},
        "queues": [],
        "tasks": [],
        "windows": {
            "1h": {"generated_at": now, "queues": {}, "partial": {}, "tasks": {}},
            "24h": {
                "generated_at": gen_24h,
                "queues": {"old": {"counters": {"dispatched": 1}, "latency": {}}},
                "partial": {"old": 0},
                "tasks": {},
            },
        },
    }


def _latest_written(client, key):
    calls = [call for call in client.set.call_args_list if call[0][0] == key]
    assert calls, f"publisher never wrote {key}"
    return json.loads(calls[-1][0][1])


def _heartbeat_written(client, key):
    calls = [call for call in client.set.call_args_list if call[0][0] == key]
    assert calls, "publisher never updated the heartbeat key"
    return calls[-1][0][1]


class TestStatsPublisher:
    def test_publish_writes_compact_payload(self, sp):
        client = MagicMock()
        client.set.return_value = True
        snapshot = _make_snapshot()
        with patch("backend.db_periodic_task.dispatch.routing.global_conn", return_value=client), patch.object(
            DispatchStats, "snapshot", return_value=snapshot
        ):
            sp.publish_dispatch_stats()

        payload = _latest_written(client, sp.KEY_LATEST)
        assert payload["schema_version"] == 1
        assert payload["queues"][0]["namespace"] == "ai"
        assert payload["queues"][0]["pending"] == 10
        assert payload["queues"][0]["budget"] == 15
        assert payload["tasks"][0]["task_key"] == "ai.task"
        # Compact: reservoir sample arrays and full registered metadata never exported.
        assert "samples" not in json.dumps(payload)
        assert "registered" not in payload

    def test_heartbeat_updated_after_lock_acquire(self, sp):
        client = MagicMock()
        client.set.return_value = True
        with patch("backend.db_periodic_task.dispatch.routing.global_conn", return_value=client), patch.object(
            DispatchStats, "snapshot", return_value=_make_snapshot()
        ):
            sp.publish_dispatch_stats()
        float(_heartbeat_written(client, sp.KEY_HEARTBEAT))  # heartbeat holds a parseable epoch

    def test_publish_skips_when_lock_held_by_another_owner(self, sp):
        client = MagicMock()
        client.set.return_value = False  # SET NX fails: another publisher holds the lock
        with patch("backend.db_periodic_task.dispatch.routing.global_conn", return_value=client), patch.object(
            DispatchStats, "snapshot"
        ) as snapshot_mock:
            sp.publish_dispatch_stats()
        snapshot_mock.assert_not_called()
        latest_calls = [c for c in client.set.call_args_list if c[0][0] == sp.KEY_LATEST]
        assert not latest_calls

    def test_lock_uses_owner_value_and_ttl(self, sp):
        client = MagicMock()
        client.set.return_value = True
        with patch("backend.db_periodic_task.dispatch.routing.global_conn", return_value=client), patch.object(
            DispatchStats, "snapshot", return_value=_make_snapshot()
        ):
            sp.publish_dispatch_stats()
        lock_calls = [c for c in client.set.call_args_list if c[0][0] == sp.KEY_PUBLISHER_LOCK]
        assert lock_calls
        owner = lock_calls[0][0][1]
        assert owner.startswith("stats_publisher:")
        assert lock_calls[0].kwargs == {"nx": True, "ex": 60}

    def test_24h_reused_when_recent(self, sp):
        now = time.time()
        client = MagicMock()
        client.set.return_value = True
        client.get.return_value = json.dumps(_existing_payload(now, age_24h=0))
        snapshot = _make_snapshot()
        with patch("backend.db_periodic_task.dispatch.routing.global_conn", return_value=client), patch.object(
            DispatchStats, "snapshot", return_value=snapshot
        ) as snapshot_mock:
            sp.publish_dispatch_stats()

        # 24h is fresh (< 5min): only the 1h snapshot is recomputed.
        assert snapshot_mock.call_count == 1
        payload = _latest_written(client, sp.KEY_LATEST)
        assert payload["windows"]["24h"]["queues"] == {"old": {"counters": {"dispatched": 1}, "latency": {}}}

    def test_24h_recomputed_when_stale(self, sp):
        now = time.time()
        client = MagicMock()
        client.set.return_value = True
        client.get.return_value = json.dumps(_existing_payload(now, age_24h=600))
        snap_24h = _make_snapshot(timestamp=now, tasks=[])
        with patch("backend.db_periodic_task.dispatch.routing.global_conn", return_value=client), patch.object(
            DispatchStats,
            "snapshot",
            side_effect=lambda *, window_seconds, **_: (
                snap_24h if window_seconds == 24 * 3600 else _make_snapshot(timestamp=now)
            ),
        ) as snapshot_mock:
            sp.publish_dispatch_stats()

        assert snapshot_mock.call_count == 2
        payload = _latest_written(client, sp.KEY_LATEST)
        assert payload["windows"]["24h"]["generated_at"] == now

    def test_24h_failure_reuses_previous_success(self, sp):
        now = time.time()
        existing = _existing_payload(now, age_24h=600)
        client = MagicMock()
        client.set.return_value = True
        client.get.return_value = json.dumps(existing)
        with patch("backend.db_periodic_task.dispatch.routing.global_conn", return_value=client), patch.object(
            DispatchStats,
            "snapshot",
            side_effect=lambda *, window_seconds, **_: (
                (_ for _ in ()).throw(RuntimeError("redis boom"))
                if window_seconds == 24 * 3600
                else _make_snapshot(timestamp=now)
            ),
        ):
            sp.publish_dispatch_stats()

        payload = _latest_written(client, sp.KEY_LATEST)
        # 1h refreshed, 24h kept from the previous successful publish.
        assert payload["windows"]["1h"]["queues"]["ai"]["counters"]["enqueued"] == 100
        assert payload["windows"]["24h"] == existing["windows"]["24h"]

    def test_1h_failure_leaves_cache_untouched(self, sp):
        client = MagicMock()
        client.set.return_value = True
        with patch("backend.db_periodic_task.dispatch.routing.global_conn", return_value=client), patch.object(
            DispatchStats, "snapshot", side_effect=RuntimeError("redis boom")
        ):
            sp.publish_dispatch_stats()
        latest_calls = [c for c in client.set.call_args_list if c[0][0] == sp.KEY_LATEST]
        assert not latest_calls

    def test_task_cap_truncates_and_marks_partial(self, sp):
        tasks = [_make_task_report(task_key=f"t{i}", ns="ai") for i in range(sp.MAX_TASK_EXPORTS + 10)]
        snapshot = _make_snapshot(tasks=tasks)
        payload = sp.build_payload(snapshot, None)
        assert len(payload["tasks"]) == sp.MAX_TASK_EXPORTS
        assert len(payload["windows"]["1h"]["tasks"]) == sp.MAX_TASK_EXPORTS
        # The namespace owning truncated tasks is flagged partial.
        assert payload["windows"]["1h"]["partial"]["ai"] == 1

    def test_unavailable_values_become_null(self, sp):
        queue = _make_queue_report(pending_total=-1, inflight=-1, config={})
        snapshot = _make_snapshot(queues=[queue])
        payload = sp.build_payload(snapshot, None)
        assert payload["queues"][0]["pending"] is None
        assert payload["queues"][0]["inflight"] is None
        assert payload["queues"][0]["max_admitted_jobs"] is None

    def test_partial_flag_surfaces(self, sp):
        queue = _make_queue_report(partial=True, diagnosis=["metrics_partial"])
        snapshot = _make_snapshot(queues=[queue])
        payload = sp.build_payload(snapshot, None)
        assert payload["windows"]["1h"]["partial"]["ai"] == 1

    def test_payload_size_warning(self, sp, caplog):
        client = MagicMock()
        client.set.return_value = True
        with patch("backend.db_periodic_task.dispatch.routing.global_conn", return_value=client), patch.object(
            DispatchStats, "snapshot", return_value=_make_snapshot()
        ), patch.object(sp, "PAYLOAD_WARN_BYTES", 1):
            sp.publish_dispatch_stats()
        assert "exceeds" in caplog.text

    def test_periodic_task_registered(self, sp):
        assert callable(sp.dispatch_publish_stats)

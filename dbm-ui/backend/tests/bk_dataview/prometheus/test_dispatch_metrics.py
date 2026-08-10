import json
import time
from unittest.mock import MagicMock, patch

from prometheus_client import CollectorRegistry

from backend.bk_dataview.prometheus.dispatch_metrics import KEY_HEARTBEAT, KEY_LATEST, DispatchMetricsCollector


class InMemoryRedis:
    """Minimal dict-backed Redis stub for collector tests."""

    def __init__(self, initial=None, *, raises=False):
        self.data = dict(initial or {})
        self.raises = raises

    def _check(self):
        if self.raises:
            raise RuntimeError("redis down")

    def set(self, key, value, ex=None, nx=False):
        self._check()
        if nx and key in self.data:
            return False
        self.data[key] = value
        return True

    def get(self, key):
        self._check()
        return self.data.get(key)


def _payload(gen_1h=None, gen_24h=None):
    now = time.time()
    return {
        "schema_version": 1,
        "generated_at": {"1h": gen_1h or now, "24h": gen_24h or now},
        "queues": [
            {
                "namespace": "ai",
                "pending": 10,
                "pending_ready": 5,
                "pending_delaying": 3,
                "inflight": 2,
                "max_admitted_jobs": 100,
                "max_inflight": 20,
                "budget": 15,
                "flow_window": 10,
                "congestion_window": 8,
                "pump_paused": 1,
                "producer_paused": 0,
                "inflight_saturated": 1,
            }
        ],
        "tasks": [{"task_key": "ai.task", "namespace": "ai", "pending": 2, "inflight": 1}],
        "windows": {
            "1h": {
                "generated_at": now,
                "queues": {
                    "ai": {
                        "counters": {"enqueued": 100, "bogus_event": 7},
                        "latency": {
                            "queue_wait_seconds": {"p50": 1.2, "p95": 3.4, "p99": 5.6},
                            "execution_seconds": {"p50": None},
                        },
                    }
                },
                "partial": {"ai": 0},
                "tasks": {
                    "ai.task": {"namespace": "ai", "outcomes": {"success": 30, "bogus_outcome": 1}, "partial": 0}
                },
            },
            "24h": {
                "generated_at": now,
                "queues": {"ai": {"counters": {"dispatched": 500}, "latency": {}}},
                "partial": {"ai": 1},
                "tasks": {},
            },
        },
    }


def _collect(collector):
    registry = CollectorRegistry()
    registry.register(collector)
    return list(registry.collect())


def _by_name(families):
    return {family.name: family for family in families}


def _health_status(families):
    health = _by_name(families).get("dbm_dispatch_collector_health")
    if health is None:
        return None
    for sample in health.samples:
        if sample.value == 1.0:
            return sample.labels["status"]
    return None


class TestDispatchMetricsCollector:
    def test_describe_lists_metric_names_without_redis(self):
        client = InMemoryRedis(raises=True)
        collector = DispatchMetricsCollector(client=client)
        # describe() must not touch Redis (raises=True would fail the test).
        names = {family.name for family in collector.describe()}
        assert "dbm_dispatch_collector_health" in names
        assert "dbm_dispatch_pending" in names
        assert "dbm_dispatch_window_events" in names
        assert "dbm_dispatch_task_outcome" in names
        assert "dbm_dispatch_publisher_heartbeat_timestamp_seconds" in names
        assert all(not family.samples for family in collector.describe())

    def test_generates_samples_from_payload(self):
        client = InMemoryRedis(initial={KEY_LATEST: json.dumps(_payload())})
        collector = DispatchMetricsCollector(client=client)
        families = _collect(collector)

        assert _health_status(families) == "ok"
        by_name = _by_name(families)

        # live gauge
        pending = list(by_name["dbm_dispatch_pending"].samples)
        assert pending[0].labels == {"namespace": "ai"}
        assert pending[0].value == 10.0

        # capacity / control gauges map payload keys to metric names
        budget = list(by_name["dbm_dispatch_budget"].samples)
        assert budget[0].value == 15.0
        assert list(by_name["dbm_dispatch_pump_paused"].samples)[0].value == 1.0

        # window events: whitelisted events only
        events = list(by_name["dbm_dispatch_window_events"].samples)
        event_keys = {(s.labels["event"], s.labels["window"], s.value) for s in events}
        assert ("enqueued", "1h", 100.0) in event_keys
        assert ("dispatched", "24h", 500.0) in event_keys
        assert all(s.labels["event"] != "bogus_event" for s in events)

        # latency: stage/quantile labels
        latency = list(by_name["dbm_dispatch_latency_seconds"].samples)
        assert any(
            s.labels["stage"] == "queue_wait"
            and s.labels["quantile"] == "p50"
            and s.labels["window"] == "1h"
            and s.value == 1.2
            for s in latency
        )

        # tasks: pending/inflight live + outcome per window (whitelisted)
        task_pending = list(by_name["dbm_dispatch_task_pending"].samples)
        assert task_pending[0].labels == {"namespace": "ai", "task_key": "ai.task"}
        assert task_pending[0].value == 2.0
        outcomes = list(by_name["dbm_dispatch_task_outcome"].samples)
        assert any(s.labels["outcome"] == "success" and s.labels["window"] == "1h" for s in outcomes)
        assert all(s.labels["outcome"] != "bogus_outcome" for s in outcomes)

        # partial + refresh freshness
        partial = list(by_name["dbm_dispatch_report_partial"].samples)
        assert any(s.labels["namespace"] == "ai" and s.labels["window"] == "24h" and s.value == 1.0 for s in partial)
        refresh = list(by_name["dbm_dispatch_refresh_timestamp_seconds"].samples)
        assert {s.labels["window"] for s in refresh} == {"1h", "24h"}

        # heartbeat emits the stored epoch when present
        assert "dbm_dispatch_publisher_heartbeat_timestamp_seconds" in by_name

    def test_heartbeat_emits_stored_epoch(self):
        now = time.time()
        client = InMemoryRedis(initial={KEY_LATEST: json.dumps(_payload()), KEY_HEARTBEAT: str(now)})
        collector = DispatchMetricsCollector(client=client)
        families = _collect(collector)
        heartbeat = list(_by_name(families)["dbm_dispatch_publisher_heartbeat_timestamp_seconds"].samples)
        assert heartbeat and heartbeat[0].value == now

    def test_lease_holder_only_emits_data(self):
        client = InMemoryRedis(initial={KEY_LATEST: json.dumps(_payload())})
        first = DispatchMetricsCollector(client=client)
        second = DispatchMetricsCollector(client=client)
        assert _collect(first)  # acquires the current 30s slot
        assert _collect(second) == []  # same slot: lease already held

    def test_cache_miss_health(self):
        client = InMemoryRedis()
        collector = DispatchMetricsCollector(client=client)
        families = _collect(collector)
        assert _health_status(families) == "cache_miss"

    def test_parse_error_health(self):
        client = InMemoryRedis(initial={KEY_LATEST: "not json"})
        collector = DispatchMetricsCollector(client=client)
        families = _collect(collector)
        assert _health_status(families) == "parse_error"

    def test_stale_1h_health(self):
        stale = time.time() - 120
        client = InMemoryRedis(initial={KEY_LATEST: json.dumps(_payload(gen_1h=stale))})
        collector = DispatchMetricsCollector(client=client)
        families = _collect(collector)
        assert _health_status(families) == "cache_stale"

    def test_redis_error_health(self):
        client = InMemoryRedis(raises=True)
        collector = DispatchMetricsCollector(client=client)
        families = _collect(collector)
        assert _health_status(families) == "redis_error"

    def test_collector_never_raises_into_registry(self):
        client = InMemoryRedis(raises=True)
        collector = DispatchMetricsCollector(client=client)
        registry = CollectorRegistry()
        registry.register(collector)
        # A raise here would fail the test — fail-open is the contract.
        list(registry.collect())

    def test_custom_registry_is_isolated(self):
        client = InMemoryRedis(initial={KEY_LATEST: json.dumps(_payload())})
        collector = DispatchMetricsCollector(client=client)
        registry = CollectorRegistry()
        registry.register(collector)
        names = {family.name for family in registry.collect()}
        # Dispatch metrics present, unrelated default-registry metrics absent.
        assert "dbm_dispatch_pending" in names
        assert "pipeline_node_execute_failed_total" not in names


class TestRegisterDispatchCollector:
    def test_register_is_idempotent(self):
        import backend.bk_dataview.prometheus.config as cfg

        fake_registry = MagicMock()
        with (
            patch.object(cfg, "_dispatch_collector_registered", False),
            patch.object(cfg, "_dispatch_collector", None),
            patch("prometheus_client.REGISTRY", fake_registry),
        ):
            cfg.register_dispatch_collector()
            cfg.register_dispatch_collector()

        assert fake_registry.register.call_count == 1

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

import hashlib
import logging
import math
import random
import time
from dataclasses import dataclass, field
from typing import Iterable, Optional

from backend.db_periodic_task.dispatch.config import PUMP_INTERVAL_SECONDS
from backend.db_periodic_task.dispatch.outcomes import DispatchOutcomeType
from backend.utils.redis import RedisConn

logger = logging.getLogger("root")

METRICS_RETENTION_SECONDS = 25 * 60 * 60
METRICS_WINDOW_SECONDS = 24 * 60 * 60
RESERVOIR_SIZE = 128
DISTRIBUTION_SAMPLE_DENOMINATOR = 10
HOUR_SECONDS = 60 * 60
MINUTE_SECONDS = 60

KEY_QUEUE_METRICS_PREFIX = "dispatch:metrics:queue:"
KEY_TASK_METRICS_PREFIX = "dispatch:metrics:task:"
KEY_SAMPLE_METRICS_PREFIX = "dispatch:metrics:sample:"

# Known per-tick queue counter names. ``queue_tick_counts`` HMGETs these instead of
# HGETALL on the hourly hash (which accumulates thousands of fields). Add new
# queue-tick counter names here when introducing them.
QUEUE_TICK_COUNTER_NAMES = (
    "candidates",
    "reserved",
    "dispatched",
    "blocked",
    "missing",
    "publish_failed",
    "completed",
    "congestion",
    "enqueued",
    "enqueue_duplicate",
    "enqueue_capacity_rejected",
    "enqueue_unavailable",
    "celery_failure",
    # Control-plane skips: filled on the next successful pump (missed) or lock fail.
    "pump_missed",
    "pump_lock_skip",
)

# Subset read by AIMD each pump tick.
AIMD_TICK_COUNTER_NAMES = (
    "dispatched",
    "completed",
    "candidates",
    "congestion",
    "blocked",
    "publish_failed",
)

RECORD_COUNTER_LUA = """
local value = redis.call('HINCRBY', KEYS[1], ARGV[1], ARGV[2])
redis.call('EXPIRE', KEYS[1], ARGV[3])
return value
"""

RECORD_RESERVOIR_LUA = """
local seen_field = 'r:' .. ARGV[1] .. ':seen'
local seen = redis.call('HINCRBY', KEYS[1], seen_field, 1)
local capacity = tonumber(ARGV[3])
local random_value = tonumber(ARGV[5])
local slot = (random_value % seen) + 1
if slot <= capacity then
    redis.call('HSET', KEYS[1], 'r:' .. ARGV[1] .. ':s:' .. slot, ARGV[2])
end
redis.call('EXPIRE', KEYS[1], ARGV[4])
return seen
"""

RECORD_RESERVOIR_BATCH_LUA = """
local metric_name = ARGV[1]
local capacity = tonumber(ARGV[2])
local retention = tonumber(ARGV[3])
local sample_count = tonumber(ARGV[4])
local seen_field = 'r:' .. metric_name .. ':seen'
local seen = 0

for index = 1, sample_count do
    local arg_offset = 4 + ((index - 1) * 2)
    local sample_value = ARGV[arg_offset + 1]
    local random_value = tonumber(ARGV[arg_offset + 2])
    seen = redis.call('HINCRBY', KEYS[1], seen_field, 1)
    local slot = (random_value % seen) + 1
    if slot <= capacity then
        redis.call('HSET', KEYS[1], 'r:' .. metric_name .. ':s:' .. slot, sample_value)
    end
end
redis.call('EXPIRE', KEYS[1], retention)
return seen
"""


def _text(value) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def tick_id(timestamp: Optional[float] = None) -> int:
    return int((time.time() if timestamp is None else timestamp) // PUMP_INTERVAL_SECONDS)


def _hour_id(timestamp: float) -> int:
    return int(timestamp // HOUR_SECONDS)


def _hour_ids(start_at: float, end_at: float) -> range:
    return range(_hour_id(start_at), _hour_id(end_at) + 1)


def _queue_key(namespace: str, hour_id: int) -> str:
    return f"{KEY_QUEUE_METRICS_PREFIX}{namespace}:{hour_id}"


def _task_key(task_key: str, hour_id: int) -> str:
    return f"{KEY_TASK_METRICS_PREFIX}{task_key}:{hour_id}"


def _sample_key(namespace: str, hour_id: int) -> str:
    return f"{KEY_SAMPLE_METRICS_PREFIX}{namespace}:{hour_id}"


def _queue_tick_field(timestamp: float, name: str) -> str:
    second_in_hour = int(timestamp) % HOUR_SECONDS
    return f"t:{second_in_hour // PUMP_INTERVAL_SECONDS}:{name}"


def _task_minute_field(timestamp: float, name: str) -> str:
    second_in_hour = int(timestamp) % HOUR_SECONDS
    return f"m:{second_in_hour // MINUTE_SECONDS}:{name}"


@dataclass
class DistributionSummary:
    count: int = 0
    samples: list[float] = field(default_factory=list)
    p50: Optional[float] = None
    p95: Optional[float] = None
    p99: Optional[float] = None


class DispatchMetrics:
    """Bounded, fail-open Redis metrics for dispatch lifecycle events."""

    _counter_script = None
    _reservoir_script = None
    _reservoir_batch_script = None

    @classmethod
    def enqueue_counter_spec(
        cls,
        namespace: str,
        task_key: str,
        *,
        timestamp: Optional[float] = None,
    ) -> tuple[str, str, str, str, int]:
        """Return Redis keys/field prefixes for atomic admission metrics."""
        observed_at = time.time() if timestamp is None else timestamp
        return (
            _task_key(task_key, _hour_id(observed_at)),
            _queue_key(namespace, _hour_id(observed_at)),
            _task_minute_field(observed_at, ""),
            _queue_tick_field(observed_at, ""),
            METRICS_RETENTION_SECONDS,
        )

    @classmethod
    def _get_counter_script(cls):
        if cls._counter_script is None:
            cls._counter_script = RedisConn.register_script(RECORD_COUNTER_LUA)
        return cls._counter_script

    @classmethod
    def _get_reservoir_script(cls):
        if cls._reservoir_script is None:
            cls._reservoir_script = RedisConn.register_script(RECORD_RESERVOIR_LUA)
        return cls._reservoir_script

    @classmethod
    def _get_reservoir_batch_script(cls):
        if cls._reservoir_batch_script is None:
            cls._reservoir_batch_script = RedisConn.register_script(RECORD_RESERVOIR_BATCH_LUA)
        return cls._reservoir_batch_script

    @staticmethod
    def should_sample(sample_identity: str) -> bool:
        """Select a stable 1-in-10 subset without Python's randomized hash()."""
        if not sample_identity:
            return True
        digest = hashlib.blake2s(sample_identity.encode("utf-8"), digest_size=4).digest()
        return int.from_bytes(digest, "big") % DISTRIBUTION_SAMPLE_DENOMINATOR == 0

    @classmethod
    def _record_counter(cls, key: str, field_name: str, amount: int = 1, *, client=None) -> None:
        cls._get_counter_script()(
            keys=[key],
            args=[field_name, int(amount), METRICS_RETENTION_SECONDS],
            client=client,
        )

    @classmethod
    def record_queue_counter(
        cls,
        namespace: str,
        name: str,
        amount: int = 1,
        *,
        timestamp: Optional[float] = None,
        client=None,
    ) -> None:
        observed_at = time.time() if timestamp is None else timestamp
        try:
            cls._record_counter(
                _queue_key(namespace, _hour_id(observed_at)),
                _queue_tick_field(observed_at, name),
                amount,
                client=client,
            )
        except Exception as exc:
            logger.debug("dispatch metrics: queue counter failed namespace=%s name=%s: %s", namespace, name, exc)

    @classmethod
    def record_task_counter(
        cls,
        task_key: str,
        name: str,
        amount: int = 1,
        *,
        timestamp: Optional[float] = None,
        client=None,
    ) -> None:
        observed_at = time.time() if timestamp is None else timestamp
        try:
            cls._record_counter(
                _task_key(task_key, _hour_id(observed_at)),
                _task_minute_field(observed_at, name),
                amount,
                client=client,
            )
        except Exception as exc:
            logger.debug("dispatch metrics: task counter failed task_key=%s name=%s: %s", task_key, name, exc)

    @classmethod
    def record_enqueue_outcome(cls, namespace: str, task_key: str, name: str, amount: int = 1) -> None:
        """Record a producer outcome when admission Lua could not do so."""
        try:
            pipe = RedisConn.pipeline(transaction=False)
            cls.record_task_counter(task_key, name, amount=amount, client=pipe)
            cls.record_queue_counter(namespace, name, amount=amount, client=pipe)
            pipe.execute()
        except Exception as exc:
            logger.debug("dispatch metrics: enqueue outcome failed task_key=%s name=%s: %s", task_key, name, exc)

    @classmethod
    def record_sample(
        cls,
        namespace: str,
        metric_name: str,
        value: float,
        *,
        timestamp: Optional[float] = None,
        client=None,
        sample_identity: str = "",
    ) -> None:
        if not math.isfinite(float(value)) or float(value) < 0:
            return
        if sample_identity and not cls.should_sample(sample_identity):
            return
        observed_at = time.time() if timestamp is None else timestamp
        try:
            cls._get_reservoir_script()(
                keys=[_sample_key(namespace, _hour_id(observed_at))],
                args=[
                    metric_name,
                    f"{observed_at}:{float(value)!r}",
                    RESERVOIR_SIZE,
                    METRICS_RETENTION_SECONDS,
                    random.getrandbits(31),
                ],
                client=client,
            )
        except Exception as exc:
            logger.debug("dispatch metrics: reservoir failed namespace=%s metric=%s: %s", namespace, metric_name, exc)

    @classmethod
    def record_samples(
        cls,
        namespace: str,
        metric_name: str,
        samples: list[tuple[str, float]],
        *,
        timestamp: Optional[float] = None,
        client=None,
    ) -> None:
        """Record a bounded deterministic subset in one reservoir script call."""
        observed_at = time.time() if timestamp is None else timestamp
        selected = [
            float(value)
            for sample_identity, value in samples
            if cls.should_sample(sample_identity) and math.isfinite(float(value)) and float(value) >= 0
        ]
        if not selected:
            return
        args: list[object] = [
            metric_name,
            RESERVOIR_SIZE,
            METRICS_RETENTION_SECONDS,
            len(selected),
        ]
        for value in selected:
            args.extend([f"{observed_at}:{value!r}", random.getrandbits(31)])
        try:
            cls._get_reservoir_batch_script()(
                keys=[_sample_key(namespace, _hour_id(observed_at))],
                args=args,
                client=client,
            )
        except Exception as exc:
            logger.debug(
                "dispatch metrics: batch reservoir failed namespace=%s metric=%s: %s", namespace, metric_name, exc
            )

    @classmethod
    def record_task_outcome(
        cls,
        namespace: str,
        task_key: str,
        outcome: DispatchOutcomeType,
        *,
        elapsed_seconds: float = -1.0,
        completed: bool = True,
        congested: bool = False,
        timestamp: Optional[float] = None,
        sample_identity: str = "",
    ) -> None:
        observed_at = time.time() if timestamp is None else timestamp
        try:
            pipe = RedisConn.pipeline(transaction=False)
            cls.record_task_counter(
                task_key,
                f"outcome:{outcome.value}",
                timestamp=observed_at,
                client=pipe,
            )
            if completed:
                cls.record_queue_counter(namespace, "completed", timestamp=observed_at, client=pipe)
            if congested:
                cls.record_queue_counter(namespace, "congestion", timestamp=observed_at, client=pipe)
            if elapsed_seconds >= 0:
                cls.record_sample(
                    namespace,
                    "execution_seconds",
                    elapsed_seconds,
                    timestamp=observed_at,
                    client=pipe,
                    sample_identity=sample_identity,
                )
            pipe.execute()
        except Exception as exc:
            logger.debug("dispatch metrics: outcome failed task_key=%s outcome=%s: %s", task_key, outcome, exc)

    @classmethod
    def queue_tick_counts(
        cls,
        namespace: str,
        observed_tick_id: int,
        *,
        names: Iterable[str] = QUEUE_TICK_COUNTER_NAMES,
    ) -> dict[str, int]:
        """Load one tick's queue counters via HMGET (not HGETALL).

        The hourly metrics hash grows with every tick field; AIMD only needs the
        previous tick's handful of counters, so point-read known field names.
        """
        timestamp = observed_tick_id * PUMP_INTERVAL_SECONDS
        field_names = tuple(names)
        if not field_names:
            return {}
        redis_fields = [_queue_tick_field(timestamp, name) for name in field_names]
        values = RedisConn.hmget(_queue_key(namespace, _hour_id(timestamp)), redis_fields) or []
        result: dict[str, int] = {}
        for name, raw_value in zip(field_names, values):
            if raw_value is None:
                continue
            result[name] = int(_text(raw_value))
        return result

    @classmethod
    def aggregate_queue_counters(
        cls,
        namespace: str,
        *,
        start_at: float,
        end_at: Optional[float] = None,
    ) -> dict[str, int]:
        end_at = time.time() if end_at is None else end_at
        result: dict[str, int] = {}
        for hour_id in _hour_ids(start_at, end_at):
            try:
                raw = RedisConn.hgetall(_queue_key(namespace, hour_id)) or {}
            except Exception:
                continue
            for raw_field, raw_value in raw.items():
                field_name = _text(raw_field)
                if not field_name.startswith("t:"):
                    continue
                _, slot, name = field_name.split(":", 2)
                bucket_at = hour_id * HOUR_SECONDS + int(slot) * PUMP_INTERVAL_SECONDS
                if start_at <= bucket_at <= end_at:
                    result[name] = result.get(name, 0) + int(raw_value)
        return result

    @classmethod
    def aggregate_task_counters(
        cls,
        task_key: str,
        *,
        start_at: float,
        end_at: Optional[float] = None,
    ) -> dict[str, int]:
        end_at = time.time() if end_at is None else end_at
        result: dict[str, int] = {}
        for hour_id in _hour_ids(start_at, end_at):
            try:
                raw = RedisConn.hgetall(_task_key(task_key, hour_id)) or {}
            except Exception:
                continue
            for raw_field, raw_value in raw.items():
                field_name = _text(raw_field)
                if not field_name.startswith("m:"):
                    continue
                _, slot, name = field_name.split(":", 2)
                bucket_at = hour_id * HOUR_SECONDS + int(slot) * MINUTE_SECONDS
                if start_at <= bucket_at <= end_at:
                    result[name] = result.get(name, 0) + int(raw_value)
        return result

    @classmethod
    def distribution(
        cls,
        namespace: str,
        metric_name: str,
        *,
        start_at: float,
        end_at: Optional[float] = None,
    ) -> DistributionSummary:
        end_at = time.time() if end_at is None else end_at
        samples: list[float] = []
        seen = 0
        for hour_id in _hour_ids(start_at, end_at):
            try:
                raw = RedisConn.hgetall(_sample_key(namespace, hour_id)) or {}
            except Exception:
                continue
            hour_seen = int(raw.get(f"r:{metric_name}:seen", raw.get(f"r:{metric_name}:seen".encode(), 0)) or 0)
            prefix = f"r:{metric_name}:s:"
            hour_samples: list[tuple[float, float]] = []
            for raw_field, raw_value in raw.items():
                if _text(raw_field).startswith(prefix):
                    try:
                        sample_at, sample_value = _text(raw_value).split(":", 1)
                        hour_samples.append((float(sample_at), float(sample_value)))
                    except (TypeError, ValueError):
                        continue
            included = [value for sample_at, value in hour_samples if start_at <= sample_at <= end_at]
            samples.extend(included)
            if included:
                hour_start = hour_id * HOUR_SECONDS
                hour_end = hour_start + HOUR_SECONDS
                if start_at <= hour_start and hour_end <= end_at:
                    seen += hour_seen
                elif hour_samples:
                    seen += round(hour_seen * len(included) / len(hour_samples))
        samples.sort()
        return DistributionSummary(
            count=seen,
            samples=samples,
            p50=cls._percentile(samples, 0.50),
            p95=cls._percentile(samples, 0.95),
            p99=cls._percentile(samples, 0.99),
        )

    @staticmethod
    def _percentile(values: Iterable[float], percentile: float) -> Optional[float]:
        ordered = list(values)
        if not ordered:
            return None
        index = min(len(ordered) - 1, max(0, math.ceil(len(ordered) * percentile) - 1))
        return ordered[index]

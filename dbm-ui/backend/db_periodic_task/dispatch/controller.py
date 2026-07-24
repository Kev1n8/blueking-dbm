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
import math
import time
from dataclasses import asdict, dataclass
from typing import Any, Optional

from backend.db_periodic_task.dispatch.config import DispatchQueueConfig
from backend.db_periodic_task.dispatch.metrics import AIMD_TICK_COUNTER_NAMES, DispatchMetrics, tick_id
from backend.db_periodic_task.dispatch.queue import DispatchQueue
from backend.utils.redis import RedisConn

logger = logging.getLogger("root")

MD_FACTOR = 0.5
CONTROLLER_STATE_TTL_SECONDS = 10 * 60
KEY_CONTROLLER_PREFIX = "dispatch:controller:"

AIMD_INCREASE = "increase"
AIMD_DECREASE = "decrease"
AIMD_HOLD = "hold"
AIMD_COLD_START = "cold_start"


def _decode_mapping(raw: dict) -> dict[str, str]:
    return {
        key.decode()
        if isinstance(key, bytes)
        else str(key): (value.decode() if isinstance(value, bytes) else str(value))
        for key, value in raw.items()
    }


def _additive_increase_step(max_inflight: int) -> int:
    """AIMD additive-increase step: 5% of configured queue concurrency."""
    return max(1, int(max_inflight) // 20)


def _cold_start_window(max_inflight: int) -> int:
    """Initial congestion window: 10% of configured queue concurrency."""
    return max(1, int(max_inflight) // 10)


@dataclass
class PumpControlDecision:
    namespace: str
    tick_id: int
    effective_budget: int
    flow_window: int
    congestion_window: int
    available_slots: int
    previous_dispatched: int = 0
    previous_completed: int = 0
    previous_demand: int = 0
    previous_congestion: int = 0
    previous_blocked: int = 0
    previous_publish_failed: int = 0
    aimd_action: str = AIMD_HOLD
    cold_start: bool = False
    partial: bool = False


class PumpController:
    """Select a bounded queue budget via flow window + congestion-window AIMD."""

    @staticmethod
    def state_key(namespace: str) -> str:
        return f"{KEY_CONTROLLER_PREFIX}{namespace}"

    @classmethod
    def decide(
        cls,
        queue_cls: type[DispatchQueue],
        config: DispatchQueueConfig,
        *,
        current_tick_id: Optional[int] = None,
    ) -> PumpControlDecision:
        current_tick_id = tick_id() if current_tick_id is None else int(current_tick_id)
        namespace = queue_cls.namespace
        inflight = queue_cls.inflight_count()
        partial = inflight < 0
        if partial:
            inflight = 0
        max_inflight = int(config.max_inflight)
        available_slots = max(0, max_inflight - inflight)
        flow_window = available_slots
        if flow_window <= 0:
            decision = PumpControlDecision(
                namespace=namespace,
                tick_id=current_tick_id,
                effective_budget=0,
                flow_window=0,
                congestion_window=0,
                available_slots=available_slots,
                aimd_action=AIMD_HOLD,
                partial=partial,
            )
            cls._persist(decision)
            return decision

        increase_step = _additive_increase_step(max_inflight)
        cold_window = _cold_start_window(max_inflight)
        try:
            state = _decode_mapping(RedisConn.hgetall(cls.state_key(namespace)) or {})
            previous = DispatchMetrics.queue_tick_counts(
                namespace,
                current_tick_id - 1,
                names=AIMD_TICK_COUNTER_NAMES,
            )
            last_tick_id = int(state.get("tick_id", -1))
            stale = last_tick_id < current_tick_id - 2
            previous_dispatched = int(previous.get("dispatched", 0))
            previous_completed = int(previous.get("completed", 0))
            previous_demand = int(previous.get("candidates", 0))
            previous_congestion = int(previous.get("congestion", 0))
            previous_blocked = int(previous.get("blocked", 0))
            previous_publish_failed = int(previous.get("publish_failed", 0))
            if not state or stale:
                congestion_window = cold_window
                decision = PumpControlDecision(
                    namespace=namespace,
                    tick_id=current_tick_id,
                    effective_budget=min(flow_window, congestion_window),
                    flow_window=flow_window,
                    congestion_window=congestion_window,
                    available_slots=available_slots,
                    previous_dispatched=previous_dispatched,
                    previous_completed=previous_completed,
                    previous_demand=previous_demand,
                    previous_congestion=previous_congestion,
                    previous_blocked=previous_blocked,
                    previous_publish_failed=previous_publish_failed,
                    aimd_action=AIMD_COLD_START,
                    cold_start=True,
                    partial=partial,
                )
            else:
                decision = cls._warm_decision(
                    namespace,
                    current_tick_id,
                    flow_window,
                    available_slots,
                    max_inflight,
                    increase_step,
                    cold_window,
                    state,
                    previous,
                    partial,
                )
        except Exception as exc:
            logger.warning("dispatch controller[%s]: fallback to AIMD cold start: %s", namespace, exc)
            congestion_window = cold_window
            decision = PumpControlDecision(
                namespace=namespace,
                tick_id=current_tick_id,
                effective_budget=min(flow_window, congestion_window),
                flow_window=flow_window,
                congestion_window=congestion_window,
                available_slots=available_slots,
                aimd_action=AIMD_COLD_START,
                cold_start=True,
                partial=True,
            )
        cls._persist(decision)
        return decision

    @classmethod
    def _warm_decision(
        cls,
        namespace: str,
        current_tick_id: int,
        flow_window: int,
        available_slots: int,
        max_inflight: int,
        increase_step: int,
        cold_window: int,
        state: dict[str, str],
        previous: dict[str, int],
        partial: bool,
    ) -> PumpControlDecision:
        previous_congestion_window = int(state.get("congestion_window", 0) or 0)
        previous_effective_budget = int(state.get("effective_budget", 0) or 0)
        previous_dispatched = int(previous.get("dispatched", 0))
        previous_completed = int(previous.get("completed", 0))
        previous_demand = int(previous.get("candidates", 0))
        previous_congestion = int(previous.get("congestion", 0))
        previous_blocked = int(previous.get("blocked", 0))
        previous_publish_failed = int(previous.get("publish_failed", 0))

        congestion_window = previous_congestion_window if previous_congestion_window > 0 else cold_window
        if previous_congestion > 0:
            congestion_window = max(1, math.floor(congestion_window * MD_FACTOR))
            aimd_action = AIMD_DECREASE
        elif (
            previous_effective_budget > 0
            # Grow only when cwnd itself was the binding constraint and the whole
            # window was dispatched with demand to spare. When flow_window (free
            # inflight slots) binds instead, cwnd holds — possibly long-term at
            # the cold-start value. That conservatism is deliberate: we accept a
            # slower ramp after sustained pressure rather than over-admit into a
            # downstream that is still draining.
            and previous_effective_budget == previous_congestion_window
            and previous_dispatched >= previous_effective_budget
            and previous_demand >= previous_effective_budget
        ):
            congestion_window = min(max_inflight, congestion_window + increase_step)
            aimd_action = AIMD_INCREASE
        else:
            aimd_action = AIMD_HOLD

        return PumpControlDecision(
            namespace=namespace,
            tick_id=current_tick_id,
            effective_budget=min(flow_window, congestion_window),
            flow_window=flow_window,
            congestion_window=congestion_window,
            available_slots=available_slots,
            previous_dispatched=previous_dispatched,
            previous_completed=previous_completed,
            previous_demand=previous_demand,
            previous_congestion=previous_congestion,
            previous_blocked=previous_blocked,
            previous_publish_failed=previous_publish_failed,
            aimd_action=aimd_action,
            partial=partial,
        )

    @classmethod
    def _persist(cls, decision: PumpControlDecision) -> None:
        try:
            mapping = {
                name: int(value) if isinstance(value, bool) else value for name, value in asdict(decision).items()
            }
            mapping["updated_at"] = time.time()
            pipe = RedisConn.pipeline(transaction=False)
            pipe.hset(cls.state_key(decision.namespace), mapping=mapping)
            pipe.expire(cls.state_key(decision.namespace), CONTROLLER_STATE_TTL_SECONDS)
            pipe.execute()
        except Exception as exc:
            logger.debug("dispatch controller[%s]: state write failed: %s", decision.namespace, exc)

    @classmethod
    def read_state(cls, namespace: str) -> dict[str, Any]:
        try:
            state: dict[str, Any] = _decode_mapping(RedisConn.hgetall(cls.state_key(namespace)) or {})
            for name in (
                "tick_id",
                "effective_budget",
                "flow_window",
                "congestion_window",
                "available_slots",
                "previous_dispatched",
                "previous_completed",
                "previous_demand",
                "previous_congestion",
                "previous_blocked",
                "previous_publish_failed",
            ):
                if name in state:
                    state[name] = int(state[name])
            if "updated_at" in state:
                state["updated_at"] = float(state["updated_at"])
            for name in ("cold_start", "partial"):
                if name in state:
                    state[name] = bool(int(state[name]))
            return state
        except Exception:
            return {}

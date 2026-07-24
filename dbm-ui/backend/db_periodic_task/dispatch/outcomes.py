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
from dataclasses import dataclass, field
from typing import Any, Optional

from blue_krill.data_types.enum import EnumField, StrStructuredEnum


class DispatchOutcomeType(StrStructuredEnum):
    """Structured outcome tags for log aggregation and observability counters."""

    SUCCESS = EnumField("success", "Work item completed successfully")
    TIMEOUT_INVOKE = EnumField("timeout_invoke", "Handler invoke timed out")
    RATELIMIT_RETRY = EnumField("ratelimit_retry", "Rate limited; job requeued")
    RATELIMIT_GAVE_UP = EnumField("ratelimit_gave_up", "Rate limited; retries exhausted")
    ERROR = EnumField("error", "Unhandled execution error")
    SKIPPED = EnumField("skipped", "Skipped before execute")
    ENQUEUED = EnumField("enqueued", "Producer enqueued work item")
    ENQUEUE_DUPLICATE = EnumField("enqueue_duplicate", "Producer skipped enqueue due to dedupe")
    ENQUEUE_CAPACITY_REJECTED = EnumField("enqueue_capacity_rejected", "Queue admitted capacity exhausted")
    ENQUEUE_DEADLINE_EXPIRED = EnumField(
        "enqueue_deadline_expired",
        "Wait deadline expired before the job could be enqueued; never retried",
    )
    ENQUEUE_UNAVAILABLE = EnumField("enqueue_unavailable", "Enqueue unavailable because Redis capacity is unproven")
    EXPIRED = EnumField("expired", "Orphaned or stale job discarded")


@dataclass
class DispatchOutcome:
    """Result of executing a single dispatched work item."""

    outcome: DispatchOutcomeType
    response: Any = None
    error: Optional[Exception] = None
    elapsed_seconds: float = -1.0
    should_requeue: bool = False
    requeue_cooldown_seconds: int = 60
    extra: dict = field(default_factory=dict)

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
from abc import abstractmethod
from typing import Any, Optional

from backend.db_periodic_task.dispatch.base import DispatchTask
from backend.db_periodic_task.dispatch.job import DispatchJob
from backend.db_periodic_task.dispatch.outcomes import DispatchOutcome
from backend.dbm_aiagent.agent.constants import DBMAgentCode
from backend.dbm_aiagent.tasks.config import AITaskConfig
from backend.dbm_aiagent.tasks.invoker import AgentInvoker, AgentRequest
from backend.dbm_aiagent.tasks.queue import AITaskQueue

__all__ = ["AITask"]


class AITask(DispatchTask):
    """AI adapter: builds agent requests and invokes AgentHandler.

    Extension guide:
    1. Subclass ``AITask`` and implement ``build_request``.
    2. Register with ``@ai_task(...)``.
    3. Submit work via ``submit(items)``; observe via ``pendings`` / ``stats``.
    4. For periodic fan-out, write a caller-owned ``@register_periodic_task``
       that selects work items and calls ``task.submit(items)``.

    You normally do not implement ``execute`` or ``_build_job`` here:
    ``AITask`` uses ``AgentInvoker`` for execution, handles 429 requeue,
    resolves agent timeouts, and calls ``handle_result`` after completion.

    Optional hooks inherited from ``DispatchTask``:
    - ``skip_before_invoke`` for worker-side stale-state checks.
    - ``on_execute_complete`` / ``handle_result`` for post-completion side effects.

    Item selection belongs in the producer before ``submit()``.
    AI jobs share ``AITaskQueue.namespace`` and that queue's per-tick / concurrency limits.
    """

    queue_cls = AITaskQueue
    agent_code: DBMAgentCode = None
    config_cls: type[AITaskConfig] = AITaskConfig

    @abstractmethod
    def build_request(self, item: Any, *, overrides: Optional[dict] = None) -> AgentRequest:
        """Build a content request for a single work item."""

    def handle_result(self, item: Any, outcome: DispatchOutcome, *, overrides: Optional[dict] = None) -> None:
        """Optional post-invoke hook."""

    def on_execute_complete(
        self,
        item: Any,
        outcome: DispatchOutcome,
        *,
        job: Optional[DispatchJob] = None,
        overrides: Optional[dict] = None,
    ) -> None:
        self.handle_result(item, outcome, overrides=overrides)

    def _resolve_request(
        self,
        item: Any,
        *,
        overrides: Optional[dict] = None,
        job: Optional[DispatchJob] = None,
    ) -> AgentRequest:
        overrides = overrides or {}
        if job is not None and job.payload_json:
            request = AgentInvoker.deserialize_request(job.payload_json)
        else:
            request = self.build_request(item, overrides=overrides)
        if overrides.get("session_code"):
            request.session_code = overrides["session_code"]
        return request

    def _build_job(
        self,
        item: Any,
        *,
        overrides: Optional[dict] = None,
        execute_at: Optional[float] = None,
        config: Optional[AITaskConfig] = None,
    ) -> DispatchJob:
        overrides = overrides or {}
        request = self._resolve_request(item, overrides=overrides)
        config = config or self._config_for_job(overrides)

        # Only freeze config on the job when submit() carried an explicit ``config``
        # key (including ``{}``); otherwise leave it empty and resolve the live DB
        # config at execute/pump time. This avoids duplicating the full config JSON
        # on every queued job (~50MB per 100k backlog for redis agent checks).
        config_json = json.dumps(config.to_raw(), ensure_ascii=False) if "config" in overrides else ""

        return self.build_queue_job(
            work_item_id=self.work_item_id(item),
            work_item_data=self.work_item_data(item),
            payload_json=AgentInvoker.serialize_request(request),
            config_json=config_json,
            execute_at=execute_at,
        )

    def execute(
        self,
        item: Any,
        *,
        job: Optional[DispatchJob] = None,
        overrides: Optional[dict] = None,
    ) -> DispatchOutcome:
        overrides = overrides or {}
        request = self._resolve_request(item, overrides=overrides, job=job)
        config = self._config_for_job(overrides)
        invoke_timeout = overrides.get("invoke_timeout_seconds", config.agent_invoke_timeout_seconds)
        retry_count = int(job.retry_count) if job is not None else int(overrides.get("retry_count", 0))

        return AgentInvoker.invoke(
            task_label=self.task_key,
            agent_code=self.agent_code,
            request=request,
            invoke_timeout_seconds=invoke_timeout,
            retry_count=retry_count,
            max_rate_limit_retries=config.max_rate_limit_retries,
            rate_limit_cooldown_seconds=config.rate_limit_cooldown_seconds,
            work_item_ref=self.work_item_id(item),
        )

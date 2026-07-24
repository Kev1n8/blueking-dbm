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

import time
from unittest.mock import MagicMock, patch

from backend.db_periodic_task.dispatch.admission import QueueAdmission
from backend.db_periodic_task.dispatch.job import DispatchJob, build_job_id
from backend.db_periodic_task.dispatch.lifecycle import QueueLifecycle
from backend.db_periodic_task.dispatch.observability import DispatchStats
from backend.db_periodic_task.dispatch.outcomes import DispatchOutcome
from backend.db_periodic_task.dispatch.reservation import ReservationStatus
from backend.dbm_aiagent.tasks.config import AITaskQueueConfig
from backend.dbm_aiagent.tasks.invoker import AgentInvoker, AgentRequest
from backend.dbm_aiagent.tasks.outcomes import AgentOutcome, DispatchOutcomeType
from backend.dbm_aiagent.tasks.queue import AITaskQueue


class TestAgentInvoker:
    def test_agent_outcome_is_a_dispatch_outcome(self):
        assert issubclass(AgentOutcome, DispatchOutcome)

    def test_success_logs_outcome(self, caplog):
        with patch("backend.dbm_aiagent.agent.handlers.AgentHandler.ask_agent_with_content", return_value="ok"):
            caplog.set_level("INFO")
            outcome = AgentInvoker.invoke(
                task_label="test.task",
                agent_code="ai-x",
                request=AgentRequest(content="hello"),
                invoke_timeout_seconds=30,
                work_item_ref="cluster:1",
            )
        assert outcome.outcome == DispatchOutcomeType.SUCCESS
        assert f"outcome={DispatchOutcomeType.SUCCESS}" in caplog.text

    def test_rate_limit_signals_requeue(self, caplog):
        class _RateLimited(Exception):
            status_code = 429

        with patch(
            "backend.dbm_aiagent.agent.handlers.AgentHandler.ask_agent_with_content",
            side_effect=_RateLimited("too many requests"),
        ):
            caplog.set_level("WARNING")
            outcome = AgentInvoker.invoke(
                task_label="test.task",
                agent_code="ai-x",
                request=AgentRequest(content="hello"),
                invoke_timeout_seconds=30,
                retry_count=0,
                max_rate_limit_retries=3,
                work_item_ref="cluster:1",
            )
        assert outcome.should_requeue is True
        assert outcome.outcome == DispatchOutcomeType.RATELIMIT_RETRY

    def test_message_containing_429_is_not_rate_limit(self):
        # Port / instance id / stack text with "429" must not trigger requeue.
        with patch(
            "backend.dbm_aiagent.agent.handlers.AgentHandler.ask_agent_with_content",
            side_effect=Exception("connect 1.1.1.1:429 failed"),
        ):
            outcome = AgentInvoker.invoke(
                task_label="test.task",
                agent_code="ai-x",
                request=AgentRequest(content="hello"),
                invoke_timeout_seconds=30,
                retry_count=0,
                max_rate_limit_retries=3,
                work_item_ref="cluster:1",
            )
        assert outcome.should_requeue is False
        assert outcome.outcome == DispatchOutcomeType.ERROR

    def test_requests_timeout_maps_to_timeout_invoke(self):
        from requests.exceptions import Timeout as RequestsTimeout

        with patch(
            "backend.dbm_aiagent.agent.handlers.AgentHandler.ask_agent_with_content",
            side_effect=RequestsTimeout("read timed out"),
        ):
            outcome = AgentInvoker.invoke(
                task_label="test.task",
                agent_code="ai-x",
                request=AgentRequest(content="hello"),
                invoke_timeout_seconds=30,
                work_item_ref="cluster:1",
            )
        assert outcome.outcome == DispatchOutcomeType.TIMEOUT_INVOKE

    def test_session_request_uses_session_handler(self):
        request = AgentRequest(content="hello", session_code="session-1")
        with patch(
            "backend.dbm_aiagent.agent.handlers.AgentHandler.ask_agent_with_content_in_session",
            return_value=("ok", "session-1"),
        ) as invoke:
            AgentInvoker.invoke(
                task_label="test.task",
                agent_code="ai-x",
                request=request,
                invoke_timeout_seconds=30,
            )
        invoke.assert_called_once()


class TestAITaskQueue:
    def test_build_job_id_stable(self):
        assert build_job_id("redis.test", "cluster:1") == "redis.test:cluster:1"

    def test_enqueue_fail_closed_on_redis_error(self):
        from backend.db_periodic_task.dispatch.admission import EnqueueStatus

        job = DispatchJob(
            job_id="j1",
            task_key="t",
            namespace="ai",
            work_item_id="cluster:1",
            created_at=time.time(),
            execute_at=time.time(),
        )
        with patch(
            "backend.db_periodic_task.dispatch.admission.QueueAdmission._enqueue_script",
            side_effect=RuntimeError("redis down"),
        ):
            statuses = QueueAdmission.enqueue_jobs(
                queue_cls=AITaskQueue,
                jobs=[job],
                dedupe_enqueue=False,
                job_ttls=[60],
                max_admitted_jobs=1,
            )
        assert statuses == [EnqueueStatus.UNAVAILABLE]

    def test_ai_queue_marks_rate_limit_as_congestion(self):
        assert AITaskQueue.is_congestion_outcome(DispatchOutcomeType.RATELIMIT_RETRY) is True
        assert AITaskQueue.is_congestion_outcome(DispatchOutcomeType.RATELIMIT_GAVE_UP) is True
        assert AITaskQueue.is_congestion_outcome(DispatchOutcomeType.TIMEOUT_INVOKE) is False
        assert AITaskQueue.is_congestion_outcome(DispatchOutcomeType.ERROR) is False

    def test_reserve_jobs_fails_closed_on_redis_error(self):
        from backend.db_periodic_task.dispatch.reservation import QueueReservation

        config = AITaskQueueConfig()
        job = DispatchJob(
            job_id="j1",
            task_key="t",
            namespace="ai",
            work_item_id="cluster:1",
        )
        with patch(
            "backend.db_periodic_task.dispatch.reservation.QueueReservation._reserve_script",
            side_effect=RuntimeError("redis down"),
        ):
            statuses = QueueReservation.reserve_jobs(
                [job],
                config,
                queue_cls=AITaskQueue,
                record_ttls=[60],
                tick_id=123,
                tick_budget=10,
            )
        assert statuses == [ReservationStatus.BLOCKED]


class TestConfigJsonDedup:
    @staticmethod
    def _task():
        from backend.dbm_aiagent.tasks.base import AITask
        from backend.dbm_aiagent.tasks.config import AITaskConfig

        class _T(AITask):
            task_key = "test.cfgjson"
            namespace = "ai"
            config_cls = AITaskConfig
            agent_code = None

            def build_request(self, item, *, overrides=None):
                return AgentRequest(content=str(item))

        return _T(config=AITaskConfig())

    def test_no_override_leaves_config_json_empty(self):
        # Common path: config resolves from DB at execute time, not frozen per job.
        job = self._task()._build_job(1, overrides={})
        assert job.config_json == ""

    def test_config_override_is_frozen_on_job(self):
        import json

        job = self._task()._build_job(1, overrides={"config": {"max_rate_limit_retries": 7}})
        assert job.config_json != ""
        assert json.loads(job.config_json)["max_rate_limit_retries"] == 7

    def test_explicit_empty_config_override_freezes_config_json(self):
        import json

        # ``config={}`` is falsy but still an explicit override — freeze the snapshot.
        job = self._task()._build_job(1, overrides={"config": {}})
        assert job.config_json != ""
        assert isinstance(json.loads(job.config_json), dict)

    def test_session_code_is_owned_by_serialized_agent_request(self):
        job = self._task()._build_job(1, overrides={"session_code": "session-1"})

        assert AgentInvoker.deserialize_request(job.payload_json).session_code == "session-1"
        assert not hasattr(job, "session_code")


class TestDispatchStats:
    def test_parse_raw_roundtrip(self):
        raw = {
            "timestamp": 1.0,
            "tick_seconds": 10,
            "pending_total": 1,
            "pending_ready": 1,
            "pending_delaying": 0,
            "inflight": 0,
            "registered": {},
            "queues": [],
            "outcomes_by_task": [],
        }
        snap = DispatchStats.parse_raw(raw)
        assert snap.pending_total == 1


class TestRegistry:
    def test_redis_checks_registered(self, django_db_blocker):
        with django_db_blocker.unblock():
            from backend.db_periodic_task.dispatch.registry import DISPATCH_REGISTRY
            from backend.db_periodic_task.local_tasks.redis_tasks.agent_checks.check_backend_data_skew import (
                CheckBackendDataSkewTask,
            )

        assert CheckBackendDataSkewTask.task_key in DISPATCH_REGISTRY
        assert DISPATCH_REGISTRY[CheckBackendDataSkewTask.task_key] is CheckBackendDataSkewTask


class TestDispatchTaskSubmit:
    def test_submit_single_and_batch(self):
        from backend.db_periodic_task.dispatch.admission import EnqueueStatus
        from backend.db_periodic_task.dispatch.base import DispatchTask
        from backend.db_periodic_task.dispatch.outcomes import DispatchOutcomeType
        from backend.db_periodic_task.dispatch.queue import DispatchQueue

        class _Task(DispatchTask):
            task_key = "test.submit"
            namespace = "test"

            def execute(self, item, *, job=None, overrides=None):
                from backend.db_periodic_task.dispatch.outcomes import DispatchOutcome, DispatchOutcomeType

                return DispatchOutcome(outcome=DispatchOutcomeType.SUCCESS)

            def _build_job(self, item, *, overrides=None, execute_at=None, config=None):
                return DispatchQueue.new_job(
                    task_key=self.task_key,
                    work_item_id=str(item),
                    work_item_data={"value": item},
                )

        task = _Task()
        with patch.object(
            QueueAdmission,
            "enqueue_jobs",
            side_effect=[
                [EnqueueStatus.ACCEPTED],
                [EnqueueStatus.ACCEPTED, EnqueueStatus.CAPACITY_REJECTED],
            ],
        ):
            single = task.submit(1)
            batch = task.submit([2, 3])
        assert len(single) == 1
        assert single[0].outcome == DispatchOutcomeType.ENQUEUED
        assert len(batch) == 2
        assert batch[0].outcome == DispatchOutcomeType.ENQUEUED
        assert batch[1].outcome == DispatchOutcomeType.ENQUEUE_CAPACITY_REJECTED

    def test_submit_loads_queue_config_once_and_chunks_by_25(self):
        from backend.db_periodic_task.dispatch.admission import EnqueueStatus
        from backend.db_periodic_task.dispatch.base import DispatchTask
        from backend.db_periodic_task.dispatch.config import DispatchQueueConfig
        from backend.db_periodic_task.dispatch.outcomes import DispatchOutcome, DispatchOutcomeType
        from backend.db_periodic_task.dispatch.queue import DispatchQueue

        class _Task(DispatchTask):
            task_key = "test.batch"
            namespace = "test"

            def execute(self, item, *, job=None, overrides=None):
                return DispatchOutcome(outcome=DispatchOutcomeType.SUCCESS)

            def _build_job(self, item, *, overrides=None, execute_at=None, config=None):
                return DispatchQueue.new_job(
                    task_key=self.task_key,
                    work_item_id=str(item),
                    work_item_data={"value": item},
                )

        task = _Task()
        with patch.object(
            DispatchQueue,
            "load_config",
            return_value=DispatchQueueConfig(max_admitted_jobs=100),
        ) as load_config, patch.object(
            QueueAdmission,
            "enqueue_jobs",
            side_effect=[
                [EnqueueStatus.ACCEPTED] * 25,
                [EnqueueStatus.ACCEPTED],
            ],
        ) as enqueue_jobs:
            outcomes = task.submit(list(range(26)))

        assert len(outcomes) == 26
        assert all(outcome.outcome == DispatchOutcomeType.ENQUEUED for outcome in outcomes)
        load_config.assert_called_once()
        assert enqueue_jobs.call_count == 2
        assert len(enqueue_jobs.call_args_list[0].kwargs["jobs"]) == 25
        assert len(enqueue_jobs.call_args_list[1].kwargs["jobs"]) == 1

    def test_requeue_outcome_is_recorded_once(self):
        from backend.db_periodic_task.dispatch.base import DispatchTask
        from backend.db_periodic_task.dispatch.config import DispatchTaskConfig
        from backend.db_periodic_task.dispatch.outcomes import DispatchOutcomeType

        class _Task(DispatchTask):
            task_key = "test.retry"
            namespace = "test"

            def execute(self, item, *, job=None, overrides=None):
                return DispatchOutcome(
                    outcome=DispatchOutcomeType.RATELIMIT_RETRY,
                    should_requeue=True,
                    requeue_cooldown_seconds=1,
                )

            def _build_job(self, item, *, overrides=None, execute_at=None, config=None):
                raise NotImplementedError

        task = _Task(config=DispatchTaskConfig(max_rate_limit_retries=3))
        task.record_outcome = MagicMock()
        task.requeue_job = MagicMock(return_value=True)
        job = DispatchJob(
            job_id="test.retry:item",
            task_key=task.task_key,
            namespace=task.namespace,
            work_item_id="item",
            work_item_data={"value": "item"},
        )

        assert task.execute_from_job(job) is True

        task.record_outcome.assert_called_once()
        assert task.record_outcome.call_args.args == (DispatchOutcomeType.RATELIMIT_RETRY,)
        assert task.record_outcome.call_args.kwargs["elapsed_seconds"] >= 0
        task.requeue_job.assert_called_once()

    def test_execute_job_preserves_successfully_requeued_job(self):
        from backend.db_periodic_task.dispatch.base import DispatchTask
        from backend.db_periodic_task.dispatch.queue import DispatchQueue
        from backend.db_periodic_task.dispatch.registry import DISPATCH_REGISTRY

        class _Task(DispatchTask):
            task_key = "test.retry.cleanup"
            namespace = "default"

            def execute(self, item, *, job=None, overrides=None):
                return DispatchOutcome(
                    outcome=DispatchOutcomeType.RATELIMIT_RETRY,
                    should_requeue=True,
                    requeue_cooldown_seconds=1,
                )

            def _build_job(self, item, *, overrides=None, execute_at=None, config=None):
                raise NotImplementedError

        job = DispatchJob(
            job_id=f"{_Task.task_key}:item",
            task_key=_Task.task_key,
            namespace="default",
            work_item_id="item",
            work_item_data={"value": "item"},
        )
        with patch.dict(DISPATCH_REGISTRY, {_Task.task_key: _Task}), patch.object(
            DispatchQueue,
            "get_job",
            return_value=job,
        ), patch.object(DispatchQueue, "record_outcome",), patch.object(
            QueueLifecycle,
            "requeue",
            return_value=True,
        ), patch.object(
            QueueLifecycle,
            "finalize_job",
        ) as finalize:
            DispatchTask.execute_job(job.job_id)

        finalize.assert_not_called()

    def test_execute_job_finalizes_terminal_job_once(self):
        from backend.db_periodic_task.dispatch.base import DispatchTask
        from backend.db_periodic_task.dispatch.queue import DispatchQueue
        from backend.db_periodic_task.dispatch.registry import DISPATCH_REGISTRY

        class _Task(DispatchTask):
            task_key = "test.success.cleanup"
            namespace = "default"

            def execute(self, item, *, job=None, overrides=None):
                return DispatchOutcome(outcome=DispatchOutcomeType.SUCCESS)

            def _build_job(self, item, *, overrides=None, execute_at=None, config=None):
                raise NotImplementedError

        job = DispatchJob(
            job_id=f"{_Task.task_key}:item",
            task_key=_Task.task_key,
            namespace="default",
            work_item_id="item",
            work_item_data={"value": "item"},
        )
        with patch.dict(DISPATCH_REGISTRY, {_Task.task_key: _Task}), patch.object(
            DispatchQueue,
            "get_job",
            return_value=job,
        ), patch.object(DispatchQueue, "record_outcome",), patch.object(
            QueueLifecycle,
            "finalize_job",
        ) as finalize:
            DispatchTask.execute_job(job.job_id)

        finalize.assert_called_once_with(
            queue_cls=DispatchQueue,
            job_id=job.job_id,
            task_key=job.task_key,
            work_item_id=job.work_item_id,
        )

    def test_execute_job_drops_unavailable_config(self):
        from backend.db_periodic_task.dispatch.base import DispatchTask
        from backend.db_periodic_task.dispatch.queue import DispatchQueue
        from backend.db_periodic_task.dispatch.registry import DISPATCH_REGISTRY

        class _Task(DispatchTask):
            task_key = "test.config.retry"
            namespace = "default"

            def execute(self, item, *, job=None, overrides=None):
                raise AssertionError("execute should not run")

            def _build_job(self, item, *, overrides=None, execute_at=None, config=None):
                raise NotImplementedError

        job = DispatchJob(
            job_id=f"{_Task.task_key}:item",
            task_key=_Task.task_key,
            namespace="default",
            work_item_id="item",
        )
        with patch.dict(DISPATCH_REGISTRY, {_Task.task_key: _Task}), patch.object(
            DispatchQueue,
            "get_job",
            return_value=job,
        ), patch.object(DispatchQueue, "config_for_job", return_value=None,), patch.object(
            DispatchQueue,
            "record_outcome",
        ) as record_outcome, patch.object(
            QueueLifecycle,
            "requeue",
        ) as requeue, patch.object(
            QueueLifecycle,
            "finalize_job",
        ) as finalize:
            DispatchTask.execute_job(job.job_id)

        record_outcome.assert_called_once()
        requeue.assert_not_called()
        finalize.assert_called_once_with(
            queue_cls=DispatchQueue,
            job_id=job.job_id,
            task_key=job.task_key,
            work_item_id=job.work_item_id,
        )

    def test_execute_job_finalizes_invalid_frozen_config(self):
        from backend.db_periodic_task.dispatch.base import DispatchTask
        from backend.db_periodic_task.dispatch.queue import DispatchQueue
        from backend.db_periodic_task.dispatch.registry import DISPATCH_REGISTRY

        class _Task(DispatchTask):
            task_key = "test.config.invalid"
            namespace = "default"

            def execute(self, item, *, job=None, overrides=None):
                raise AssertionError("execute should not run")

            def _build_job(self, item, *, overrides=None, execute_at=None, config=None):
                raise NotImplementedError

        job = DispatchJob(
            job_id=f"{_Task.task_key}:item",
            task_key=_Task.task_key,
            namespace="default",
            work_item_id="item",
            config_json="{",
        )
        with patch.dict(DISPATCH_REGISTRY, {_Task.task_key: _Task}), patch.object(
            DispatchQueue,
            "get_job",
            return_value=job,
        ), patch.object(DispatchQueue, "record_outcome",) as record_outcome, patch.object(
            QueueLifecycle,
            "requeue",
        ) as requeue, patch.object(
            QueueLifecycle,
            "finalize_job",
        ) as finalize:
            DispatchTask.execute_job(job.job_id)

        record_outcome.assert_called_once()
        requeue.assert_not_called()
        finalize.assert_called_once_with(
            queue_cls=DispatchQueue,
            job_id=job.job_id,
            task_key=job.task_key,
            work_item_id=job.work_item_id,
        )

    def test_pendings_inflights_stats(self):
        from backend.db_periodic_task.dispatch.base import DispatchTask
        from backend.db_periodic_task.dispatch.queue import DispatchQueue

        class _Task(DispatchTask):
            task_key = "test.stats"
            namespace = "test"

            def execute(self, item, *, job=None, overrides=None):
                from backend.db_periodic_task.dispatch.outcomes import DispatchOutcome, DispatchOutcomeType

                return DispatchOutcome(outcome=DispatchOutcomeType.SUCCESS)

            def _build_job(self, item, *, overrides=None, execute_at=None, config=None):
                return DispatchQueue.new_job(
                    task_key=self.task_key,
                    work_item_id=str(item),
                    work_item_data={"value": item},
                )

        task = _Task()
        with patch.object(DispatchQueue, "pending_count_for_task", return_value=2), patch.object(
            DispatchQueue, "inflight_count_for_task", return_value=1
        ), patch.object(DispatchQueue, "has_pending_for_task", return_value=True), patch.object(
            DispatchQueue, "has_inflight_for_task", return_value=False
        ), patch.object(
            DispatchQueue, "outcomes_for_task", return_value={"success": 5}
        ):
            assert task.pendings == 2
            assert task.inflights == 1
            assert task.is_idle is False
            assert task.stats == {"pending": 2, "inflight": 1, "outcomes": {"success": 5}}

    def test_is_idle_when_both_queues_empty(self):
        from backend.db_periodic_task.dispatch.base import DispatchTask
        from backend.db_periodic_task.dispatch.queue import DispatchQueue

        class _Task(DispatchTask):
            task_key = "test.idle"
            namespace = "test"

            def execute(self, item, *, job=None, overrides=None):
                from backend.db_periodic_task.dispatch.outcomes import DispatchOutcome, DispatchOutcomeType

                return DispatchOutcome(outcome=DispatchOutcomeType.SUCCESS)

            def _build_job(self, item, *, overrides=None, execute_at=None, config=None):
                return DispatchQueue.new_job(
                    task_key=self.task_key,
                    work_item_id=str(item),
                    work_item_data={"value": item},
                )

        task = _Task()
        with patch.object(DispatchQueue, "has_pending_for_task", return_value=False), patch.object(
            DispatchQueue, "has_inflight_for_task", return_value=False
        ):
            assert task.is_idle is True

    def test_ai_execution_timeout_follows_agent_invoke_timeout(self):
        from backend.dbm_aiagent.tasks.config import AITaskConfig

        cfg = AITaskConfig(agent_invoke_timeout_seconds=3600)
        assert cfg.resolve_execution_timeout_seconds() == 3600
        assert cfg.resolve_inflight_ttl_seconds() == 3660

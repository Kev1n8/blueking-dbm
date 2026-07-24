# -*- coding: utf-8 -*-
"""
Simple verification consumer for the dispatch pipeline.

Purpose: exercise enqueue → pump → worker without calling a real Agent, and
without sharing AIMD / concurrency with production queues such as ``ai``.

Lives under ``dispatch`` (not ``dbm_aiagent``) so framework validation does not
pollute the AI adapter package.

Ownership of ``DummyTaskQueue`` / namespace ``dummy``:
- Only ``DummyTask`` (``dummy.smoke``) may use this queue.
- Do **not** point other ``DispatchTask`` / ``AITask`` subclasses at
  ``DummyTaskQueue`` or ``namespace="dummy"``. Real work belongs on its own
  queue (e.g. ``AITaskQueue``) so verification traffic cannot starve or distort
  production budgets.
"""

import logging
import random
import time
from dataclasses import dataclass
from typing import ClassVar

from backend.db_periodic_task.dispatch.config import DispatchQueueConfig
from backend.db_periodic_task.dispatch.job import DispatchJob
from backend.db_periodic_task.dispatch.outcomes import DispatchOutcome, DispatchOutcomeType
from backend.db_periodic_task.dispatch.queue import DispatchQueue
from backend.dbm_aiagent.agent.constants import DBMAgentCode
from backend.dbm_aiagent.tasks.base import AITask
from backend.dbm_aiagent.tasks.config import AITaskConfig
from backend.dbm_aiagent.tasks.invoker import AgentRequest
from backend.dbm_aiagent.tasks.registry import ai_task

logger = logging.getLogger("root")

DUMMY_NAMESPACE = "dummy"
DUMMY_TASK_KEY = "dummy.smoke"
# Mimic typical AgentHandler latency when callers omit ``sleep``.
DUMMY_DEFAULT_SLEEP_MIN_SECONDS = 10.0
DUMMY_DEFAULT_SLEEP_MAX_SECONDS = 30.0


@dataclass
class DummyTaskQueueConfig(DispatchQueueConfig):
    """Group ceilings for the verification-only ``dummy`` queue.

    Do not reuse this namespace for production tasks; create a dedicated
    ``DispatchQueueConfig`` / ``DispatchQueue`` pair instead.
    """

    namespace: ClassVar[str] = DUMMY_NAMESPACE


class DummyTaskQueue(DispatchQueue):
    """Verification-only Redis namespace; keep production tasks off this queue.

    Reserved for ``DummyTask``. Binding other tasks here mixes verification
    injects with real work under one AIMD window — use a separate queue instead.
    """

    config_cls = DummyTaskQueueConfig

    @classmethod
    def is_congestion_outcome(cls, outcome: DispatchOutcomeType) -> bool:
        # Match AITaskQueue so ratelimit injects can exercise AIMD decrease.
        return outcome in {
            DispatchOutcomeType.RATELIMIT_RETRY,
            DispatchOutcomeType.RATELIMIT_GAVE_UP,
        }


@dataclass
class DummyTaskConfig(AITaskConfig):
    """Runtime config for ``dummy.smoke``; defaults favor verification runs."""

    task_key: ClassVar[str] = DUMMY_TASK_KEY
    enabled: bool = True


@ai_task(
    agent_code=DBMAgentCode.DBM,
    config_cls=DummyTaskConfig,
    prompt_template="dummy:{name}",
    db_type="test",
)
class DummyTask(AITask):
    """Sole consumer of ``DummyTaskQueue``: enqueue → pump → worker path checks.

    Do not subclass this for production work, and do not set
    ``queue_cls = DummyTaskQueue`` on other tasks.

    Work-item inject flags (verification only):
    - ``raise``: raise RuntimeError
    - ``sleep``: sleep seconds (default random 10–30)
    - ``ratelimit`` / ``ratelimit_until``: mimic 429 requeue until ``retry_count`` reaches until
    - ``ratelimit_always``: keep returning rate-limit until gave-up
    - ``timeout``: TIMEOUT_INVOKE (no requeue)
    - ``skip_invoke``: skip_before_invoke hook
    - ``delay_seconds`` / ``execute_at``: schedule pending score in the future
    """

    # Exclusive binding: DummyTaskQueue must stay verification-only.
    queue_cls = DummyTaskQueue

    def work_item_id(self, item) -> str:
        if isinstance(item, dict):
            return str(item.get("name") or item.get("work_item_id") or item)
        return str(item)

    def build_request(self, item, *, overrides=None) -> AgentRequest:
        name = item.get("name") if isinstance(item, dict) else item
        return AgentRequest(content=f"dummy:{name}")

    def skip_before_invoke(self, item) -> tuple[bool, str]:
        if isinstance(item, dict) and item.get("skip_invoke"):
            return True, "dummy_skip_invoke"
        return False, ""

    def _build_job(self, item, *, overrides=None, execute_at=None, config=None) -> DispatchJob:
        job = super()._build_job(item, overrides=overrides, execute_at=execute_at, config=config)
        if isinstance(item, dict):
            if item.get("execute_at") is not None:
                job.execute_at = float(item["execute_at"])
            elif item.get("delay_seconds") is not None:
                job.execute_at = time.time() + float(item["delay_seconds"])
        return job

    def execute(self, item, *, job=None, overrides=None) -> DispatchOutcome:
        if isinstance(item, dict) and item.get("raise"):
            raise RuntimeError(f"dummy forced error work_item={self.work_item_id(item)}")

        if isinstance(item, dict) and item.get("timeout"):
            return DispatchOutcome(outcome=DispatchOutcomeType.TIMEOUT_INVOKE)

        if isinstance(item, dict) and (item.get("ratelimit") or item.get("ratelimit_always")):
            retry_count = int(job.retry_count) if job is not None else 0
            max_retries = int(self.config.max_rate_limit_retries)
            cooldown = max(1, int(item.get("cooldown", 1)))
            still_limited = bool(item.get("ratelimit_always")) or retry_count < int(item.get("ratelimit_until", 1))
            if still_limited:
                if retry_count < max_retries:
                    return DispatchOutcome(
                        outcome=DispatchOutcomeType.RATELIMIT_RETRY,
                        should_requeue=True,
                        requeue_cooldown_seconds=cooldown,
                    )
                return DispatchOutcome(outcome=DispatchOutcomeType.RATELIMIT_GAVE_UP)
            # ratelimit_until exhausted → fall through to success

        if isinstance(item, dict) and item.get("sleep") is not None:
            sleep_s = float(item.get("sleep") or 0)
        else:
            sleep_s = random.uniform(DUMMY_DEFAULT_SLEEP_MIN_SECONDS, DUMMY_DEFAULT_SLEEP_MAX_SECONDS)
        if sleep_s > 0:
            time.sleep(sleep_s)
        logger.info("DummyTask done work_item=%s sleep=%.2f", self.work_item_id(item), sleep_s)
        return DispatchOutcome(outcome=DispatchOutcomeType.SUCCESS)

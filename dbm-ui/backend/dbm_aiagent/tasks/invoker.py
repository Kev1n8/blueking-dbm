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
import logging
import time
from dataclasses import asdict, dataclass
from typing import Optional, Tuple, Type

from backend.db_periodic_task.dispatch.outcomes import DispatchOutcome, DispatchOutcomeType
from backend.dbm_aiagent.agent.constants import DBMAgentCode
from backend.dbm_aiagent.tasks.config import AGENT_RESPONSE_LOG_MAX_CHARS
from backend.dbm_aiagent.tasks.outcomes import AgentOutcome

logger = logging.getLogger("root")

# Built-in TimeoutError plus requests timeouts (not subclasses of TimeoutError).
_TIMEOUT_EXC_TYPES: Tuple[Type[BaseException], ...] = (TimeoutError,)
try:
    from requests.exceptions import Timeout as _RequestsTimeout

    _TIMEOUT_EXC_TYPES = (TimeoutError, _RequestsTimeout)
except ImportError:  # pragma: no cover - requests is a hard dependency in practice
    pass


def _http_status_code(exc: Exception) -> Optional[int]:
    """Extract an HTTP status from common SDK / HTTP client exception shapes."""
    for attr in ("status_code", "status"):
        value = getattr(exc, attr, None)
        if isinstance(value, int):
            return value
    response = getattr(exc, "response", None)
    if response is not None:
        value = getattr(response, "status_code", None)
        if isinstance(value, int):
            return value
    # ApiResultError / AppBaseException may stash the HTTP status in ``code``.
    code = getattr(exc, "code", None)
    if isinstance(code, int):
        return code
    if isinstance(code, str) and code.isdigit():
        return int(code)
    return None


def is_rate_limit_error(exc: Exception) -> bool:
    """True only when the exception carries an HTTP 429 status — never free-text."""
    return _http_status_code(exc) == 429


def truncate_agent_response_for_log(response, max_chars: int = AGENT_RESPONSE_LOG_MAX_CHARS) -> str:
    if not isinstance(response, str):
        return repr(response)
    if len(response) <= max_chars:
        return repr(response)
    return f"{response[:max_chars]!r}...[truncated, total_len={len(response)}]"


@dataclass
class AgentRequest:
    """Payload sent to AgentHandler."""

    content: str = ""
    session_code: Optional[str] = None
    username: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: dict) -> "AgentRequest":
        return cls(
            content=raw.get("content", ""),
            session_code=raw.get("session_code"),
            username=raw.get("username"),
        )


class AgentInvoker:
    """Universal middle layer between AITask workers and AgentHandler."""

    @classmethod
    def invoke(
        cls,
        *,
        task_label: str,
        agent_code: DBMAgentCode,
        request: AgentRequest,
        invoke_timeout_seconds: int,
        retry_count: int = 0,
        max_rate_limit_retries: int = 0,
        rate_limit_cooldown_seconds: int = 60,
        work_item_ref: str = "",
    ) -> DispatchOutcome:
        invoke_timeout = max(1, int(invoke_timeout_seconds))
        invoke_started_at = time.monotonic()
        try:
            from backend.dbm_aiagent.agent.handlers import AgentHandler

            if request.session_code:
                ai_response, _ = AgentHandler.ask_agent_with_content_in_session(
                    agent_code=agent_code,
                    content=request.content,
                    session_code=request.session_code,
                    username=request.username,
                    timeout=invoke_timeout,
                )
            else:
                ai_response = AgentHandler.ask_agent_with_content(
                    agent_code=agent_code,
                    content=request.content,
                    username=request.username,
                    timeout=invoke_timeout,
                )
            elapsed = time.monotonic() - invoke_started_at
            logger.info(
                "%s: work_item=%s outcome=%s elapsed=%.2fs invoke_timeout=%ds agent_response=%s",
                task_label,
                work_item_ref,
                DispatchOutcomeType.SUCCESS,
                elapsed,
                invoke_timeout,
                truncate_agent_response_for_log(ai_response),
            )
            return AgentOutcome(outcome=DispatchOutcomeType.SUCCESS, response=ai_response, elapsed_seconds=elapsed)

        except _TIMEOUT_EXC_TYPES as exc:
            elapsed = time.monotonic() - invoke_started_at
            logger.warning(
                "%s: work_item=%s outcome=%s elapsed=%.2fs invoke_timeout=%ds: %s",
                task_label,
                work_item_ref,
                DispatchOutcomeType.TIMEOUT_INVOKE,
                elapsed,
                invoke_timeout,
                exc,
            )
            return AgentOutcome(outcome=DispatchOutcomeType.TIMEOUT_INVOKE, error=exc, elapsed_seconds=elapsed)

        except Exception as exc:
            elapsed = time.monotonic() - invoke_started_at
            rate_limited = is_rate_limit_error(exc)
            if rate_limited and retry_count < max_rate_limit_retries:
                cooldown = max(1, int(rate_limit_cooldown_seconds))
                logger.warning(
                    "%s: work_item=%s outcome=%s attempt=%d/%d cooldown=%ds: %s",
                    task_label,
                    work_item_ref,
                    DispatchOutcomeType.RATELIMIT_RETRY,
                    retry_count + 1,
                    max_rate_limit_retries,
                    cooldown,
                    exc,
                )
                return AgentOutcome(
                    outcome=DispatchOutcomeType.RATELIMIT_RETRY,
                    error=exc,
                    elapsed_seconds=elapsed,
                    should_requeue=True,
                    requeue_cooldown_seconds=cooldown,
                )
            if rate_limited:
                logger.error(
                    "%s: work_item=%s outcome=%s attempts=%d: %s",
                    task_label,
                    work_item_ref,
                    DispatchOutcomeType.RATELIMIT_GAVE_UP,
                    retry_count,
                    exc,
                )
                return AgentOutcome(outcome=DispatchOutcomeType.RATELIMIT_GAVE_UP, error=exc, elapsed_seconds=elapsed)

            logger.exception(
                "%s: work_item=%s outcome=%s elapsed=%.2fs invoke_timeout=%ds: %s",
                task_label,
                work_item_ref,
                DispatchOutcomeType.ERROR,
                elapsed,
                invoke_timeout,
                exc,
            )
            return AgentOutcome(outcome=DispatchOutcomeType.ERROR, error=exc, elapsed_seconds=elapsed)

    @staticmethod
    def serialize_request(request: AgentRequest) -> str:
        return json.dumps(request.to_dict(), ensure_ascii=False)

    @staticmethod
    def deserialize_request(payload: str) -> AgentRequest:
        return AgentRequest.from_dict(json.loads(payload))

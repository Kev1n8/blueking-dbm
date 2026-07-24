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
from functools import lru_cache

from backend.utils.redis import RedisConn


@lru_cache(maxsize=None)
def register_script_once(script: str):
    """Cache the Redis script object for ``script`` across callers.

    Replaces the repeated ``global _script; if _script is None: _script = ...``
    boilerplate: each module calls this once at import time and reuses the
    cached handle for every EVALSHA.
    """
    return RedisConn.register_script(script)

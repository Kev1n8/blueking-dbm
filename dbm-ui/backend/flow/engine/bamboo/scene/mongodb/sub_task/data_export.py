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
from dataclasses import dataclass
from typing import Tuple

from django.conf import settings
from django.utils.translation import ugettext as _

from backend.core.encrypt.handlers import AsymmetricCipherConfigType, AsymmetricHandler
from backend.flow.consts import MONGODB_DATA_EXPORT_PATH, MongoDBActuatorActionEnum
from backend.flow.engine.bamboo.scene.common.builder import SubBuilder
from backend.flow.engine.bamboo.scene.mongodb.sub_task.base_subtask import BaseSubTask
from backend.flow.plugins.components.collections.mongodb.exec_actuator_job2 import ExecJobComponent2
from backend.flow.utils.base.bkrepo import get_bk_repo_url
from backend.flow.utils.mongodb.mongodb_dataclass import CommonContext
from backend.flow.utils.mongodb.mongodb_repo import MongoDBCluster, MongoDBNsFilter, MongoNode, MongoNodeWithLabel
from backend.flow.utils.mongodb.mongodb_util import MongoUtil

logger = logging.getLogger("flow")


@dataclass
class ExportConfig:
    """Configuration for MongoDB data export operations."""

    ns_filter: dict
    export_options: dict
    filename: str
    file_path: str


class DataExportSubTask(BaseSubTask):
    """
    MongoDB数据导出子任务
    """

    @classmethod
    def make_kwargs(cls, cluster: MongoDBCluster, node: MongoNode, config: ExportConfig) -> dict:
        """
        Create kwargs for the MongoDB data export actuator job.
        """
        bk_dbm_instance = MongoNodeWithLabel.from_node(node, clu=cluster)
        dba_user, dba_pwd = MongoUtil().get_dba_user_password(node.ip, node.port, node.bk_cloud_id)

        is_partial = MongoDBNsFilter.is_partial(config.ns_filter)
        is_dumping = config.export_options["format"] == "bson"
        sudo_account = MongoUtil().get_mongodb_os_conf()["user"]
        db_cloud_token = AsymmetricHandler.encrypt(
            name=AsymmetricCipherConfigType.PROXYPASS, content=f"{node.bk_cloud_id}_dbactuator_token"
        )
        return {
            "set_trans_data_dataclass": CommonContext.__name__,
            "get_trans_data_ip_var": None,
            "bk_cloud_id": node.bk_cloud_id,
            "exec_ip": node.ip,
            "db_act_template": {
                "action": MongoDBActuatorActionEnum.DataExport,
                "file_path": config.file_path,
                "exec_account": "root",
                "sudo_account": sudo_account,
                "payload": {
                    "bk_dbm_instance": bk_dbm_instance.__json__(),
                    "ip": node.ip,
                    "port": int(node.port),
                    "adminUsername": dba_user,
                    "adminPassword": dba_pwd,
                    "args": {
                        "is_dumping": is_dumping,
                        "is_partial": is_partial,
                        "ns_filter": config.ns_filter,
                        "query": config.export_options.get("query"),
                        "fields": config.export_options.get("fields"),
                        "format": config.export_options.get("format"),
                    },
                    "upload_detail": {
                        "bk_cloud_id": node.bk_cloud_id,
                        "db_cloud_token": db_cloud_token,
                        "fileserver": {
                            "url": get_bk_repo_url(node.bk_cloud_id),
                            "bucket": settings.BKREPO_BUCKET,
                            "username": settings.BKREPO_USERNAME,
                            "password": settings.BKREPO_PASSWORD,
                            "project": settings.BKREPO_PROJECT,
                            "upload_path": MONGODB_DATA_EXPORT_PATH.format(biz=cluster.bk_biz_id),
                        },
                    },
                    "filename": config.filename,
                },
            },
        }

    @classmethod
    def __export_act(cls, cluster: MongoDBCluster, node: MongoNode, config: ExportConfig) -> Tuple[dict, MongoNode]:
        """
        Generate act of data export.
        """
        kwargs = cls.make_kwargs(cluster, node, config)

        return {
            "act_name": _("目标实例: {}".format(node.addr())),
            "act_component_code": ExecJobComponent2.code,
            "kwargs": kwargs,
        }

    @classmethod
    def replica_set_sub_flow(cls, root_id, data, cluster: MongoDBCluster, task_info: dict, file_path: str):
        """
        ReplicaSet Data Export
        """
        logger.info(f"Exporting data from ReplicaSet {cluster.name}")
        builder = SubBuilder(root_id=root_id, data=data)
        config = ExportConfig(
            ns_filter=task_info["ns_filter"],
            export_options=task_info["export_options"],
            filename=task_info["filename"],
            file_path=file_path,
        )

        act = cls.__export_act(cluster, task_info["node"], config)
        builder.add_act(**act)
        return builder.build_sub_process(_(f"{cluster.immute_domain}-数据导出"))

    @classmethod
    def sharded_cluster_sub_flow(cls, root_id, data, cluster: MongoDBCluster, task_info: dict, file_path: str):
        """
        ShardedCluster Data Export
        """
        logger.info(f"Exporting data from ShardedCluster {cluster.name}")
        builder = SubBuilder(root_id=root_id, data=data)
        config = ExportConfig(
            ns_filter=task_info["ns_filter"],
            export_options=task_info["export_options"],
            filename=task_info["filename"],
            file_path=file_path,
        )

        act = cls.__export_act(cluster, task_info["node"], config)
        builder.add_act(**act)
        return builder.build_sub_process(_(f"{cluster.immute_domain}-数据导出"))

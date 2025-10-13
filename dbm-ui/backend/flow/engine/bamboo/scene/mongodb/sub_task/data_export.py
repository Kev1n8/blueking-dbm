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
from typing import List, Tuple

from django.conf import settings
from django.utils.translation import ugettext as _

from backend.core.encrypt.handlers import AsymmetricCipherConfigType, AsymmetricHandler
from backend.flow.consts import MongoDBActuatorActionEnum
from backend.flow.engine.bamboo.scene.common.builder import SubBuilder, SubProcess
from backend.flow.engine.bamboo.scene.mongodb.sub_task.base_subtask import BaseSubTask
from backend.flow.plugins.components.collections.mongodb.exec_actuator_job2 import ExecJobComponent2
from backend.flow.utils.base.bkrepo import get_bk_repo_url
from backend.flow.utils.mongodb.mongodb_dataclass import CommonContext
from backend.flow.utils.mongodb.mongodb_repo import (
    MongoDBCluster,
    MongoDBNsFilter,
    MongoNode,
    MongoNodeWithLabel,
    ReplicaSet,
)
from backend.flow.utils.mongodb.mongodb_util import MongoUtil

logger = logging.getLogger("flow")


BKREPO_DATA_EXPORT_PATH = "mongodb-data-export/{biz}"


@dataclass
class ExportConfig:
    """Configuration for MongoDB data export operations."""

    ns_filter: dict
    export_options: dict
    filename_prefix: str
    file_path: str


class DataExportSubTask(BaseSubTask):
    """
    MongoDB数据导出子任务
    """

    @staticmethod
    def make_file_name(prefix: str, set_name: str):
        return f"{prefix}_{set_name}"

    @classmethod
    def make_kwargs(cls, node: MongoNode, shard: ReplicaSet, cluster: MongoDBCluster, config: ExportConfig) -> dict:
        """
        Create kwargs for the MongoDB data export actuator job.
        """
        bk_dbm_instance = MongoNodeWithLabel.from_node(node, shard, cluster)
        dba_user, dba_pwd = MongoUtil().get_dba_user_password(node.ip, node.port, node.bk_cloud_id)

        is_partial = MongoDBNsFilter.is_partial(config.ns_filter)
        is_dumping = config.export_options["format"] == "bson"
        sudo_account = MongoUtil().get_mongodb_os_conf()["user"]
        db_cloud_token = AsymmetricHandler.encrypt(
            name=AsymmetricCipherConfigType.PROXYPASS, content=f"{node.bk_cloud_id}_dbactuator_token"
        )
        filename = cls.make_file_name(config.filename_prefix, shard.set_name)
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
                            "upload_path": BKREPO_DATA_EXPORT_PATH.format(biz=cluster.bk_biz_id),
                        },
                    },
                    "filename": filename,
                },
            },
        }

    @classmethod
    def __export_shard(
        cls, cluster: MongoDBCluster, shard: ReplicaSet, config: ExportConfig
    ) -> Tuple[dict, MongoNode]:
        """
        Deliver data export job onto a shard.
        """
        nodes = shard.get_not_backup_nodes()
        if not nodes:
            raise Exception(_(f"Shard has no non-backup nodes: {shard.set_name}"))
        node: MongoNode = nodes[0]

        kwargs = cls.make_kwargs(node, shard, cluster, config)

        return {
            "act_name": _("{}: {}".format(shard.set_name, node.addr())),
            "act_component_code": ExecJobComponent2.code,
            "kwargs": kwargs,
        }, node

    @classmethod
    def replica_set_sub_flow(
        cls, root_id, ticket_data, cluster: MongoDBCluster, task_info: dict, file_path: str
    ) -> Tuple[SubProcess, List]:
        """
        ReplicaSet Data Export
        """
        logger.info(f"Exporting data from ReplicaSet {cluster.name}")

        sb = SubBuilder(root_id=root_id, data=ticket_data)
        shard: ReplicaSet = task_info["shards"][0]  # ReplicaSet only has 1 shard

        config = ExportConfig(
            ns_filter=task_info["ns_filter"],
            export_options=task_info["export_options"],
            filename_prefix=task_info["filename_prefix"],
            file_path=file_path,
        )

        act, node = cls.__export_shard(cluster, shard, config)
        sb.add_act(**act)

        filename = cls.make_file_name(config.filename_prefix, shard.set_name)
        result_files_map = {filename: f"{BKREPO_DATA_EXPORT_PATH.format(biz=cluster.bk_biz_id)}/{filename}.tar"}

        return (
            sb.build_sub_process(_(f"ReplicaSet-{shard.set_name}-数据导出")),
            [
                node.ip,
            ],
            result_files_map,
        )

    @classmethod
    def sharded_cluster_sub_flow(
        cls, root_id, ticket_data, cluster: MongoDBCluster, task_info: dict, file_path: str
    ) -> Tuple[SubBuilder, List]:
        """
        ShardedCluster Data Export
        """
        sb = SubBuilder(root_id=root_id, data=ticket_data)

        shards: ReplicaSet = task_info["shards"]

        config = ExportConfig(
            ns_filter=task_info["ns_filter"],
            export_options=task_info["export_options"],
            filename_prefix=task_info["filename_prefix"],
            file_path=file_path,
        )

        logger.info(f"Exporting data from ShardedCluster {cluster.name}")

        host_list = set()
        nodes_acts = []
        result_files_map = {}
        for shard in shards:
            act, node = cls.__export_shard(cluster, shard, config)
            filename = cls.make_file_name(config.filename_prefix, shard.set_name)
            nodes_acts.append(act)
            host_list.add(node.ip)
            result_files_map[filename] = f"{BKREPO_DATA_EXPORT_PATH.format(biz=cluster.bk_biz_id)}/{filename}.tar"

        if nodes_acts:
            sb.add_parallel_acts(nodes_acts)

        return (
            sb.build_sub_process(_("ShardedCluster-{}-数据导出").format(cluster.name)),
            list(host_list),
            result_files_map,
        )

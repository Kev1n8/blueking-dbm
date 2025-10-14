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
from typing import Dict, Optional

from django.utils.translation import ugettext as _

from backend.configuration.constants import DBType
from backend.db_meta.enums import ClusterType
from backend.flow.engine.bamboo.scene.common.builder import Builder
from backend.flow.engine.bamboo.scene.common.get_file_list import GetFileList
from backend.flow.engine.bamboo.scene.mongodb.base_flow import MongoBaseFlow
from backend.flow.engine.bamboo.scene.mongodb.sub_task.data_export import DataExportSubTask
from backend.flow.engine.bamboo.scene.mongodb.sub_task.send_media import SendMedia
from backend.flow.plugins.components.collections.mongodb.store_export_results import StoreExportResultsComponent
from backend.flow.utils.mongodb.mongodb_repo import MongoRepository
from backend.flow.utils.mongodb.mongodb_util import MongoUtil

logger = logging.getLogger("flow")


class MongoDataExportFlow(object):
    """
    MongoDB数据导出flow

    Flow流程:
    ┌──────────────────────────────────────────────┐
    │  Main Flow (export_flow)                     │
    │  - 根据 infos 整理出来 {"cluster": task_info}  │
    │  - 根据 cluster_type 调用不同 sub_flow         │
    └──────────────────────────────────────────────┘
                            │
                ┌───────────┴─────────────┐
                │                         │
                ▼                         ▼
    ┌──────────────────────┐   ┌──────────────────────────┐
    │ replica_set_sub_flow │   │ sharded_cluster_sub_flow │
    │ - Single shard       │   │ - Multiple shards        │
    │                      │   │ - Parallel export        │
    └──────────────────────┘   └──────────────────────────┘
                │                       │
                └───────────┬───────────┘
                            ▼
                    ┌────────────────┐
                    │ __export_shard │
                    │ - Select node  │
                    │ - Make kwargs  │
                    │ - Return act   │
                    └────────────────┘
                            │
                            ▼
                ┌───────────────────────────┐
                │ StoreExportResults        │
                │ - Store result files path │
                └───────────────────────────┘
    """

    def __init__(self, root_id: str, data: Optional[Dict]):
        """
        传入参数
        """

        self.root_id = root_id
        self.data = data

    def export_flow(self):
        """
        mongo_data_export 流程
        """
        logger.debug("MongoDataExportFlow start, payload", self.data)
        file_list = GetFileList(db_type=DBType.MongoDB).get_db_actuator_package()

        # Parse input and validate cluster existence
        cluster_tasks = {}
        for info in self.data.get("infos", []):
            cluster_id = info["cluster_id"]
            if cluster_id in cluster_tasks:
                raise Exception(_(f"Duplicate cluster_id found: {cluster_id}"))

            cluster = MongoRepository.fetch_one_cluster(id=cluster_id)
            if not cluster:
                raise Exception(_(f"Cluster {cluster_id} not found"))

            shards = cluster.get_shards(with_config=False)
            if not shards:
                raise Exception(_(f"Shards is empty for cluster: {cluster.immute_domain}"))
            cluster_tasks[cluster] = {
                "export_options": info.get("export_options", {}),
                "ns_filter": info["ns_filter"],
                "shards": shards,
                "filename_prefix": info["filename_prefix"],
            }

        # Process each cluster, generate subflows
        main_pipeline = Builder(root_id=self.root_id, data=self.data)
        cluster_pipelines = []
        host_list = set()
        actuator_workdir = MongoUtil().get_mongodb_os_conf()["file_path"]
        cluster_results = {}
        for cluster, task_info in cluster_tasks.items():
            MongoBaseFlow.check_cluster_valid(cluster, self.data)

            sub_flow_param = {
                "root_id": self.root_id,
                "ticket_data": self.data,
                "cluster": cluster,
                "task_info": task_info,
                "file_path": actuator_workdir,
            }
            sub_flow_func = self.get_sub_flow_func(cluster.cluster_type)
            sub_process, ips, result_map = sub_flow_func(**sub_flow_param)
            host_list.update(ips)
            cluster_pipelines.append(sub_process)
            cluster_results[cluster.cluster_id] = result_map

        # Deliver actuator package to all target hosts first
        bk_host_list = [{"ip": ip} for ip in host_list]
        main_pipeline.add_act(
            **SendMedia.act(
                act_name=_("MongoDB-介质下发({})".format(len(host_list))),
                file_list=file_list,
                bk_host_list=bk_host_list,
                file_target_path=actuator_workdir,
            )
        )

        if cluster_pipelines:
            main_pipeline.add_parallel_sub_pipeline(cluster_pipelines)

            # 导出的文件以 {"cluster_id": {"set_name": <path>}}
            # 的形式保存在 ticket["details"]["result_files_map"]
            main_pipeline.add_act(
                act_name=_("保存导出文件的信息"),
                act_component_code=StoreExportResultsComponent.code,
                kwargs={
                    "ticket_id": self.data["ticket_id"],
                    "cluster_results": cluster_results,
                },
            )

        main_pipeline.run_pipeline()

    @classmethod
    def get_sub_flow_func(cls, cluster_type: str):
        handler = cls._FLOW_HANDLERS.get(cluster_type)
        if not handler:
            raise Exception(_(f"Unknown Cluster Type: {cluster_type}"))
        return handler

    _FLOW_HANDLERS = {
        ClusterType.MongoReplicaSet: DataExportSubTask.replica_set_sub_flow,
        ClusterType.MongoShardedCluster: DataExportSubTask.sharded_cluster_sub_flow,
    }

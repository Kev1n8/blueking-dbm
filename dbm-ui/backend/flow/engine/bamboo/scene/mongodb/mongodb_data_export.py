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
from backend.flow.utils.mongodb.mongodb_repo import MongoDBCluster, MongoRepository
from backend.flow.utils.mongodb.mongodb_util import MongoUtil

logger = logging.getLogger("flow")


class MongoDataExportFlow(object):
    """
    MongoDB数据导出flow

    Flow流程:
        ┌─────────────────────────────────────────┐
        │  Main Flow (export_flow)                │
        │  1. 解析 infos 并且验证 cluster           │
        │  2. 为集群选择执行节点                     │
        │     - ReplicaSet: shard[0] 的非备份节点   │
        │     - ShardedCluster: mongos[0]         │
        │  3. cluster_tasks: {cluster: task_info} │
        └─────────────────────────────────────────┘
                            │
                            ▼
                  ┌──────────────────┐
                  │   SendMedia Act  │
                  └──────────────────┘
                            │ Export clusters in parallel
            ┌───────────────┴───────────────┐
            ▼                               ▼
    ┌──────────────────┐          ┌──────────────────┐
    │ Cluster 1 Export │          │ Cluster N Export │
    │ SubFlow          │   ...    │ SubFlow          │
    └──────────────────┘          └──────────────────┘
            └───────────────┬───────────────┘
                            ▼
                ┌───────────────────────────┐
                │ DataExportSubTask         │
                │ .export_cluster_sub_flow  │
                │ - Make kwargs             │
                │ - ExecJobComponent2       │
                └───────────────────────────┘
    """

    def __init__(self, root_id: str, data: Optional[Dict]):
        """
        传入参数
        """
        self.root_id = root_id
        self.data = data
        self.cluster_tasks = {}
        self.host_list = set()
        self.parse_infos()

    def parse_infos(self):
        """
        将 infos 解析为 {cluster: task_info}
        """
        cluster_seened = set()
        for info in self.data.get("infos", []):
            cluster_id = info["cluster_id"]
            if cluster_id in cluster_seened:
                raise Exception(_(f"Duplicate cluster_id found: {cluster_id}"))
            cluster_seened.add(cluster_id)

            cluster = MongoRepository.fetch_one_cluster(id=cluster_id)
            if not cluster:
                raise Exception(_(f"Cluster {cluster_id} not found"))
            MongoBaseFlow.check_cluster_valid(cluster, self.data)

            node = self.__pick_node(cluster)
            self.host_list.add(node.ip)
            self.cluster_tasks[cluster] = {
                "node": node,
                "export_options": info.get("export_options", {}),
                "ns_filter": info["ns_filter"],
                "filename": info["filename"],
            }
        logger.debug("MongoDataExportFlow payload parsed", self.cluster_tasks)

    def export_flow(self):
        """
        mongo_data_export 流程
        """

        main_pipeline = Builder(root_id=self.root_id, data=self.data)
        file_list = GetFileList(db_type=DBType.MongoDB).get_db_actuator_package()
        bk_host_list = [{"ip": ip} for ip in self.host_list]
        actuator_workdir = MongoUtil().get_mongodb_os_conf()["file_path"]

        # Deliver actuator package to all target hosts first
        main_pipeline.add_act(
            **SendMedia.act(
                act_name=_("MongoDB-介质下发({})".format(len(self.host_list))),
                file_list=file_list,
                bk_host_list=bk_host_list,
                file_target_path=actuator_workdir,
            )
        )

        cluster_sub_flows = []
        for cluster, task_info in self.cluster_tasks.items():
            sub_flow_param = {
                "root_id": self.root_id,
                "data": self.data,
                "cluster": cluster,
                "task_info": task_info,
                "file_path": actuator_workdir,
            }
            cluster_sub_flows.append(DataExportSubTask.export_cluster_sub_flow(**sub_flow_param))

        if cluster_sub_flows:
            main_pipeline.add_parallel_sub_pipeline(cluster_sub_flows)

        main_pipeline.run_pipeline()

    @classmethod
    def __pick_node(cls, cluster: MongoDBCluster):
        """
        选择集群目标节点
        """
        match cluster.cluster_type:
            case ClusterType.MongoReplicaSet:
                nodes = cluster.get_shards()[0].get_not_backup_nodes()
            case ClusterType.MongoShardedCluster:
                nodes = cluster.get_mongos()
            case _:
                raise Exception(_(f"Unsupported cluster type: {cluster.cluster_type}"))

        if not nodes:
            raise Exception(_(f"cluster: {cluster.immute_domain} has no valid nodes"))
        return nodes[0]

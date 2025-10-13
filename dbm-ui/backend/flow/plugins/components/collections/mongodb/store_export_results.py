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
from django.utils.translation import ugettext as _
from pipeline.component_framework.component import Component

from backend.flow.plugins.components.collections.common.base_service import BaseService
from backend.ticket.models.ticket import Ticket


class StoreExportResultsService(BaseService):
    """
    Store MongoDB export results using FlowOutputHandler
    """

    def _execute(self, data, parent_data):
        kwargs = data.get_one_of_inputs("kwargs")

        ticket: Ticket = None
        try:
            ticket_id = kwargs["ticket_id"]
            ticket = Ticket.objects.get(id=ticket_id)
        except Ticket.DoesNotExist:
            self.log_error(_("Ticket {} does not exist").format(ticket_id))
            return False

        cluster_results = kwargs.get("cluster_results", {})

        if cluster_results:
            ticket.update_details(result_files_map=cluster_results)
            self.log_info(_("成功存储 {} 个集群的导出结果").format(len(cluster_results)))
        else:
            self.log_warning(_("没有导出结果需要存储"))

        return True


class StoreExportResultsComponent(Component):
    name = __name__
    code = "store_mongodb_export_results"
    bound_service = StoreExportResultsService

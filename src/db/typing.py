# -*- coding:utf8 -*-
"""
传入的key和类型，写在db_api外，当作一个小conf一起传入，db_api根据传入自行判断接受与否
"""


from db.local_db import LocalDb as BaseDb


class PipetaskInfo(BaseDb):

    def __init__(self):
        super(PipetaskInfo, self).__init__()
        self.table_type = "pipetask_info"
        self.design_table_type.update({  # db会自动添加create_time:str和last_write_time:str两项
            "pipetask_id": str,

            "pipeline_id": str,
            "finish_node_list": list
        })
        self.map_id = "pipetask_id"


class PipelineInfo(BaseDb):

    def __init__(self):
        super(PipelineInfo, self).__init__()
        self.table_type = "pipeline_info"
        self.design_table_type.update({
            "pipeline_id": str,
            "pipeline_name": str,

            "dag_dict": dict,
            "topo_order_list": list,
            "config": None,
            "node_dict": dict
        })
        self.map_id = "pipeline_id"


class PipenodeInfo(BaseDb):

    def __init__(self):
        super(PipenodeInfo, self).__init__()
        self.table_type = "pipenode_info"
        self.design_table_type.update({
            "pipenode_id": str,
            "pipenode_name": str,

            "func_des": str,
            "func_str": str,
            "type": str,
            "inputs": list,
            "outputs": list,
            "next_nodes": list,
            "prep_nodes": list,
            "flag": None,
            "outputs_r": list
        })
        self.map_id = "pipenode_id"

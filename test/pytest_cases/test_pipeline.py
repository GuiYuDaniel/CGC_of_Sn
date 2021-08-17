# -*- coding:utf8 -*-
"""
测试pipeline能否被成功实例化，检查，调用
"""


import os
import pytest
from utils.io import Path
from utils.json_op import Json
from pipeline.pipeline import Pipeline


class TestPipeLine(object):

    def setup_class(self,):
        self.fake_path = Path._get_full_path(relative_path="fake", base_path_type="test")  # 此处没使用config避免循环引用
        self.fake_dag_workflow_file_path = os.path.join(self.fake_path, "fake_dag_workflow.json")
        self.fake_workflow_conf = Json.file_to_json_without_comments(self.fake_dag_workflow_file_path)
        self.answer_calc_dag = {"f1": {"next_nodes": ["f2"], "prep_nodes": []},
                                "f2": {"next_nodes": ["f3", "f5"], "prep_nodes": ["f1"]},
                                "f3": {"next_nodes": ["f4"], "prep_nodes": ["f2"]},
                                "f4": {"next_nodes": ["f6"], "prep_nodes": ["f3"]},
                                "f5": {"next_nodes": ["f6"], "prep_nodes": ["f2"]},
                                "f6": {"next_nodes": ["f7"], "prep_nodes": ["f4", "f5"]},
                                "f7": {"next_nodes": [], "prep_nodes": ["f6"]}}
        self.answer_topo_order_1 = ["f1", "f2", "f3", "f5", "f4", "f6", "f7"]
        self.answer_topo_order_2 = ["f1", "f2", "f5", "f3", "f4", "f6", "f7"]

    def test_pipeline_create_and_query(self):
        ppl = Pipeline(self.fake_workflow_conf)
        query_all_data = ppl.query()
        assert query_all_data.get("topo_order_list") == self.answer_topo_order_1 or \
               query_all_data.get("topo_order_list") == self.answer_topo_order_2
        assert query_all_data.get("dag_dict") == self.answer_calc_dag

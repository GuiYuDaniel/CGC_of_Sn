# -*- coding:utf8 -*-
"""
测试pipenode能否被成功实例化，检查，调用
TODO pickle化保存和装载
"""


import os
import pytest
from utils.io import Path
from utils.json_op import Json
from pipeline.pipenode import Pipenode


class TestPipeNode(object):

    def setup_class(self):
        self.top_path = Path._get_full_path()
        self.fake_path = Path._get_full_path(relative_path="fake", base_path_type="test")  # 此处没使用config避免循环引用
        self.whereami_file_path = os.path.join(self.fake_path, "fake_workflow_whereami.json")
        self.workflow_conf_dict = Json.file_to_json(self.whereami_file_path)[0]
        self.workflow_conf_dict["prep_nodes"] = []
        self.answer_query_all = {"name": "first_node",
                                 "func_des": ["None.test.fake.fake_core", "where_am_i", ""],
                                 "func_str": "from test.fake.fake_core import where_am_i",
                                 "type": "cold",
                                 "inputs": ["cmd_params"],
                                 "outputs": ["results"],
                                 "next_nodes": [],
                                 "prep_nodes": [],
                                 "outputs_r": {},
                                 "flags": []}

    def test_pipenode_create_and_query(self):
        pipenode = Pipenode(self.workflow_conf_dict)
        pipenode.check_node()
        # 检查query_all
        query_all_data = pipenode.query()
        answer_all = self.answer_query_all
        assert query_all_data == answer_all
        # 每个项逐一检查
        for k in answer_all:
            query_single = pipenode.query(name=k)
            answer_single = {k: answer_all.get(k)}
            assert query_single == answer_single

    def test_pipenode_outputs_r(self):
        pipenode = Pipenode(self.workflow_conf_dict)
        pipenode.check_node()
        # 检查outputs_r能否正常赋值和调用
        right_answer = {"results": 1}
        pipenode.outputs_r = right_answer
        assert pipenode.outputs_r == right_answer
        # 检查故意错误赋值能否被捕捉，原有正确的代码是否保持
        wrong_answer = {"rst": 1}
        pipenode.outputs_r = wrong_answer
        try:
            pipenode.outputs_r = wrong_answer
            err_msg = "outputs_r = wrong cannot be checked, should not do this code"
            raise Exception(err_msg)
        except Exception as e:
            assert pipenode.outputs_r == right_answer

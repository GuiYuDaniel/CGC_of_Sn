# -*- coding:utf8 -*-
"""
测试src/utils/json_op功能是否正确执行
"""


import os
import pytest
from utils.io import Path
from utils.json_op import Json
from pipeline.pipeline import Pipeline
from pipeline.pipetask import Pipetask


class TestPipeLine(object):

    def setup_class(self):
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

    def test_pipetask_create(self):
        ppl = Pipeline(self.fake_workflow_conf)
        ppt = Pipetask(ppl)
        ppt.start()


class TestFunc(object):
    """对于pipetask可能执行的function，做一下test，保证行为要与预期一致
    test_easy是抽出核心机制
    test是调用pipetask测"""

    def setup_class(self):
        self.fake_path = Path._get_full_path(relative_path="fake", base_path_type="test")  # 此处没使用config避免循环引用
        self.fake_wfl_output_str = os.path.join(self.fake_path, "fake_workflow_output_str.json")
        self.fake_wfl_output_str_conf = Json.file_to_json_without_comments(self.fake_wfl_output_str)
        # self.fake_wfl_output_str = os.path.join(self.fake_path, "fake_workflow_output_str.json")
        # self.fake_wfl_output_str_conf = Json.file_to_json_without_comments(self.fake_wfl_output_str)
        # self.fake_wfl_output_str = os.path.join(self.fake_path, "fake_workflow_output_str.json")
        # self.fake_wfl_output_str_conf = Json.file_to_json_without_comments(self.fake_wfl_output_str)
        # self.fake_wfl_output_str = os.path.join(self.fake_path, "fake_workflow_output_str.json")
        # self.fake_wfl_output_str_conf = Json.file_to_json_without_comments(self.fake_wfl_output_str)
        # self.fake_wfl_output_str = os.path.join(self.fake_path, "fake_workflow_output_str.json")
        # self.fake_wfl_output_str_conf = Json.file_to_json_without_comments(self.fake_wfl_output_str)
        # self.fake_wfl_output_str = os.path.join(self.fake_path, "fake_workflow_output_str.json")
        # self.fake_wfl_output_str_conf = Json.file_to_json_without_comments(self.fake_wfl_output_str)
        # self.fake_wfl_output_str = os.path.join(self.fake_path, "fake_workflow_output_str.json")
        # self.fake_wfl_output_str_conf = Json.file_to_json_without_comments(self.fake_wfl_output_str)
        # self.fake_wfl_output_str = os.path.join(self.fake_path, "fake_workflow_output_str.json")
        # self.fake_wfl_output_str_conf = Json.file_to_json_without_comments(self.fake_wfl_output_str)
        # self.fake_wfl_output_str = os.path.join(self.fake_path, "fake_workflow_output_str.json")
        # self.fake_wfl_output_str_conf = Json.file_to_json_without_comments(self.fake_wfl_output_str)
        # self.fake_wfl_output_str = os.path.join(self.fake_path, "fake_workflow_output_str.json")
        # self.fake_wfl_output_str_conf = Json.file_to_json_without_comments(self.fake_wfl_output_str)
        # self.fake_wfl_output_str = os.path.join(self.fake_path, "fake_workflow_output_str.json")
        # self.fake_wfl_output_str_conf = Json.file_to_json_without_comments(self.fake_wfl_output_str)
        # self.fake_wfl_output_str = os.path.join(self.fake_path, "fake_workflow_output_str.json")
        # self.fake_wfl_output_str_conf = Json.file_to_json_without_comments(self.fake_wfl_output_str)

    def test_easy_output_str_func(self):
        # 这个是抽出来的核心机制
        exec("from test.fake.fake_core import output_str_func")
        exec("from test.fake.fake_core import is_input_str_func")
        func_1 = eval("output_str_func")
        func_2 = eval("is_input_str_func")
        output_1 = func_1()
        output_1 = tuple([output_1])
        output_2 = func_2(*output_1)
        assert output_2

    def test_easy_output_all_str_fun(self):
        # 这个是抽出来的核心机制
        exec("from test.fake.fake_core import output_all_str_func")
        exec("from test.fake.fake_core import is_input_all_str_func")
        func_1 = eval("output_all_str_func")
        func_2 = eval("is_input_all_str_func")
        output_1 = func_1(None)
        output_1 = tuple([output_1[0], output_1[1]])
        output_2 = func_2(*output_1)
        assert output_2

    def test_output_str_func(self):
        ppl = Pipeline(self.fake_wfl_output_str_conf)
        ppt = Pipetask(ppl)
        ppt.start()
        ppn_1 = ppl.node_dict.get("f1")
        ppn_2 = ppl.node_dict.get("f2")
        assert ppn_1.outputs_r.get(ppn_1.outputs[0]) == "this is str"
        assert ppn_2.outputs_r.get(ppn_2.outputs[0])

    # def test_output_number_func(self):
    #     pass
    #
    # def test_output_list_func(self):
    #     pass
    #
    # def test_output_dict_func(self):
    #     pass
    #
    # def test_output_tuple_func(self):
    #     # python的多变量返回也是tuple，避免和单变量就是tuple情况混淆
    #     pass
    #
    # def test_output_multi_str_func_1(self):
    #     def output_str_func():
    #         return "this is str 1", "this is str 2"
    #
    #     def input_str_func(a, b):
    #         return True if isinstance(a, str) and isinstance(b, str) else False
    #
    #     func_1 = output_str_func
    #     func_2 = input_str_func
    #     output_1 = func_1()
    #     output_2 = func_2(*output_1)
    #     assert output_2
    #
    # def test_output_multi_str_fun_2(self):
    #     def output_str_func():
    #         return "this is str 1", "this is str 2"
    #
    #     def input_str_func(*args):
    #         return True if isinstance(args[0], str) and isinstance(args[1], str) else False
    #
    #     func_1 = output_str_func
    #     func_2 = input_str_func
    #     output_1 = func_1()
    #     output_2 = func_2(*output_1)
    #     assert output_2

    # def test_output_multi_number_func(self):
    #     pass
    #
    # def test_output_multi_list_func(self):
    #     pass
    #
    # def test_output_multi_dict_func(self):
    #     pass
    #
    # def test_output_multi_tuple_func(self):
    #     pass
    #
    # def test_output_multi_type_func(self):
    #     pass

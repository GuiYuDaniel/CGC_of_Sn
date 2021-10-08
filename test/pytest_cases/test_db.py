# -*- coding:utf8 -*-
"""
测试src/db下所有功能是否正确执行
"""


import copy
import numpy as np
import os
import shutil
import time
import pytest
import uuid
from db.local_db import LocalDb
from db.typing import PipetaskInfo, PipenodeInfo, PipelineInfo
from utils.io import Path
from utils.log import get_logger


logger = get_logger(__name__)


class TmpTyping(LocalDb):

    def __init__(self):
        super(TmpTyping, self).__init__()
        self.table_type = "tmp_info"
        self.design_table_type.update({  # db会自动添加create_time:str和last_write_time:str两项
            "id": str,

            "fake_str": str,
            "fake_int": int,
            "fake_float": float,
            "fake_bool": bool,
            "fake_list": list,
            "fake_dict": dict,
            "fake_tuple": tuple,
            "fake_object": object,
            "fake_np_int": np.int,
            "fake_np_float": np.float,
            "fake_none": None
        })
        self.map_id = "id"


class TestTyping(object):
    """
    test python file typing functions
    """

    def setup_class(self):
        pass


class TestLocalDb(object):
    """
    test class LocalDb functions
    """

    def setup_class(self):
        self.fake_path = Path._get_full_path(relative_path="", base_path_type="top")  # 此处没使用config避免循环引用
        self.db_folder = os.path.join(self.fake_path, "results", "tmp_info")
        if os.path.exists(self.db_folder):
            shutil.rmtree(self.db_folder)
        self.test_data = {
            "id": str(uuid.uuid4()),

            "fake_str": "str",
            "fake_int": 1,
            "fake_float": 0.01,
            "fake_bool": True,
            "fake_list": [1, 2],
            "fake_dict": {1: 1},
            "fake_tuple": (1,),
            "fake_np_int": np.int(1),
            "fake_np_float": np.float(0.01),
            "fake_none": {"err_msg": "fake"}
        }
        self.test_data_same = copy.deepcopy(self.test_data)
        self.update_data = {"fake_bool": False}
        self.update_data_same = copy.deepcopy(self.update_data)
        self.test_condition_true = {"id": self.test_data.get("id"), "fake_bool": True}
        self.test_condition_true_same = copy.deepcopy(self.test_condition_true)
        self.test_condition_false = {"id": self.test_data.get("id"), "fake_bool": False}
        self.test_condition_false_same = copy.deepcopy(self.test_condition_false)

    def teardown_class(self):
        if os.path.exists(self.db_folder):
            shutil.rmtree(self.db_folder)

    def test_insert_update_query_and_delete(self):
        db_info = TmpTyping()
        # test insert
        flag, msg = db_info.insert(self.test_data)
        assert self.test_data == self.test_data_same  # 要保证数据不会被变动
        assert flag
        assert msg is True
        # test query
        flag, data = db_info.query(self.test_condition_true)
        assert self.test_condition_true == self.test_condition_true_same
        assert flag
        assert data is not False
        for key in self.test_data:
            assert key in data
            assert self.test_data[key] == data.get(key)
        assert isinstance(data.get("create_time"), str)
        assert isinstance(data.get("last_write_time"), str)
        flag, data_same = db_info.query(self.test_condition_true)
        assert flag
        assert data_same is not False
        assert data == data_same
        assert (data is not data_same)  # 保证它们俩的地址不是一个
        # test update
        time.sleep(2)  # 睡两秒，防止秒级时间update和insert一致
        flag, msg = db_info.update(self.test_condition_false, self.update_data)  # 应该找不到
        assert self.update_data == self.update_data_same
        assert self.test_data == self.test_data_same
        assert flag
        assert msg is False
        flag, msg = db_info.update(self.test_condition_true, self.update_data)  # 应该得到
        assert self.update_data == self.update_data_same
        assert self.test_data == self.test_data_same
        assert flag
        assert msg is True
        flag, data_1 = db_info.query(self.test_condition_true)  # 这个就应该是找不到的
        assert self.test_condition_true == self.test_condition_true_same
        assert flag
        assert data_1 is False
        flag, data_1 = db_info.query(self.test_condition_false)
        assert self.test_condition_false == self.test_condition_false_same
        assert flag
        assert data_1 is not False
        assert isinstance(data_1.get("create_time"), str)
        assert isinstance(data_1.get("last_write_time"), str)
        assert data.get("create_time") == data_1.get("create_time")
        assert data.get("last_write_time") != data_1.get("last_write_time")
        assert data.get("fake_bool")
        assert data_1.get("fake_bool") is False
        del data_1["fake_bool"]
        test_data_copy = copy.deepcopy(self.test_data)
        del test_data_copy["fake_bool"]
        for key in test_data_copy:
            assert key in data_1
            assert test_data_copy[key] == data_1.get(key)
        # test delete
        flag, msg = db_info.delete(self.test_condition_true)  # 应该是找不到的
        assert self.test_condition_true == self.test_condition_true_same
        assert flag
        assert msg is False
        flag, msg = db_info.delete(self.test_condition_false)  # 可以找到
        assert self.test_condition_false == self.test_condition_false_same
        assert flag
        assert msg is True

    def test_insert_wrong(self):
        pass

    def test_update_wrong(self):
        pass

    def test_query_wrong(self):
        pass

    def test_delete_wrong(self):
        pass

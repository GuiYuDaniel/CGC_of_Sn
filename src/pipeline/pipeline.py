# -*- coding:utf8 -*-
"""
pipeline负责构建整体计算图：
（串行拓扑序调度）

1，构建顶层拓扑序（既检查了DAG性，又是串行执行顺序）

2，构建pipenodes

3，query方法等


本工作目前只考虑单线程串行，所以这是个简略版pipeline，
暂不支持：微服务，并行异步计算
"""

import copy
from pipeline.pipenode import Pipenode
from utils.config import Config
from utils.topo import (calc_dag, calc_topo_order)
from utils.log import get_logger


logger = get_logger(__name__)


class Pipeline(object):
    """
    负责检查并构建计算图，分配对应的pipenode，辅助pipetask执行
    TODO：得pickle化落盘，才能在pipeline层面实现restart
    """

    def __init__(self, workflow_conf):
        self.dag_dict = None  # 如果dag表达不够好，未来也可以换成邻接矩阵（adjacency matrix）
        self._topo_order_list = None  # 计算顺序
        self._node_dict = None  # 先把实例化的node存在这里，未来可以考虑使用db
        # config
        self.config = Config()
        # 由conf排出任务顺序
        self.dag_dict = calc_dag(workflow_conf)
        self._topo_order_list = calc_topo_order(self.dag_dict)
        # 把prep_nodes添加进workflow_conf
        for single_node_dict in workflow_conf:
            node_name = single_node_dict.get("name")
            single_node_dict["prep_nodes"] = self.dag_dict.get(node_name).get("prep_nodes")
        # 初始化所有节点
        self._node_dict = self._create_node_dict(workflow_conf)  # 注意，这里的workflow_conf已经被添加prep_nodes了
        # 初始化其他必要信息

    """ 将一些关键变量，设置为属性，不允许调用时，随意更改"""
    @property
    def topo_order_list(self):
        return self._topo_order_list

    @property
    def node_dict(self):
        return self._node_dict

    def _create_node_dict(self, workflow_conf):
        node_name_list = self._topo_order_list
        if len(node_name_list) != len(workflow_conf):
            err_msg = "node_name_list={} and workflow_conf={} must have same len".format(node_name_list, workflow_conf)
            logger.error(err_msg)
            raise Exception(err_msg)
        node_dict = {}
        workflow_conf = copy.deepcopy(workflow_conf)
        for single_dict in workflow_conf:  # 反正字典是无序化的，我们可以依w_c初始化node
            if single_dict.get("name") not in node_name_list:
                err_msg = "node_name={} not in topo_order_list={}, conf_dict={}".format(
                    single_dict.get("name"), node_name_list, single_dict)
                logger.error(err_msg)
                raise Exception(err_msg)
            # create node
            pipenode = Pipenode(single_dict)
            pipenode.check_node()
            # insert
            node_dict[single_dict.get("name")] = pipenode
        if len(node_name_list) != len(node_dict):
            err_msg = "node_name_list={} and node_dict={} must have same len".format(node_name_list, node_dict)
            logger.error(err_msg)
            logger.debug("workflow_conf={}".format(workflow_conf))
            raise Exception(err_msg)
        return node_dict

    def query(self, name="all"):
        # query不是能拿到全部的东西
        name_list = ["all", "topu_order_list", "dag_dict"]
        if name not in name_list:
            rst = {"err_msg": "query name={} must in {}".format(name, name_list)}
            logger.debug("query pipeline={} with name={}".format(rst, name))
            return rst
        if name == "all":
            rst = {"topo_order_list": self.topo_order_list,
                   "dag_dict": self.dag_dict}
            logger.debug("query pipeline={} with name={}".format(rst, name))
            return rst
        else:
            rst = {name: eval("self.{}".format(name))}
            logger.debug("query pipeline={} with name={}".format(rst, name))
            return rst

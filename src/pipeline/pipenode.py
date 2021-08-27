# -*- coding:utf8 -*-
"""
pipenode负责维护单个计算节点的所有信息，
被希望于当需要使用哪个节点明确时，访问对应ppn就可以了：

1，构建节点信息

2，更新节点信息

3，检查节点内函数实例化
"""

from utils.log import get_logger


logger = get_logger(__name__)


class Pipenode(object):
    """
    pipenode负责记录节点信息，由pipeline负责初始化。是计算到具体节点时，供pipetask访问的对象
    TODO time的记录
    TODO 暂不考虑动态装载func，即默认所有func都是存在于项目中，存在于已有PYTHONPATH里
    """

    def __init__(self, conf):
        self._name = None
        self._func_des = None
        self._func_str = None
        self._type = None
        self._inputs = None
        self._outputs = None
        self._extra_args = None  # todo
        self._extra_kwargs = None  # todo
        self._next_nodes = None
        self._prep_nodes = None
        self._flags = None
        self._outputs_r = None  # 用_r区分实际变量与描述性变量

        self._create_with_conf(conf)

    '''这个类所有参数都不应该被随意更改'''

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        # name需要为str
        if name is None or not isinstance(name, str):
            err_msg = "name={} must be str".format(name)
            logger.error(err_msg)
            raise Exception(err_msg)
        self._name = name

    @property
    def func_des(self):
        return self._func_des

    @func_des.setter
    def func_des(self, func):  # func在ppnode里存描述，便于未来落盘需要
        # func要求是形式上确保将来可以form xx import yy as zz
        if not isinstance(func, list) or len(func) != 3 or \
                any(not isinstance(i, str) for i in func):
            err_msg = "func={} must a len 3 list, and like ['None.xxx', 'yyy', 'zzz'] or ['xxx', 'yyy', 'zzz']" \
                      "in order to form xxx import yyy as zzz".format(func)
            logger.error(err_msg)
            raise Exception(err_msg)
        self._func_des = func

    @property
    def func_str(self):
        return self._func_str

    def _set_func_str(self):
        # 暂时保留None的设定
        if self.func_des[0].startswith("None"):
            tmp_func_des_0 = self.func_des[0].replace("None.", "")
        else:
            tmp_func_des_0 = self.func_des[0]
        if self.func_des[2]:
            func_str = "from {} import {} as {}".format(tmp_func_des_0, self.func_des[1], self.func_des[2])
        else:
            func_str = "from {} import {}".format(tmp_func_des_0, self.func_des[1])
        self._func_str = func_str

    def check_node(self):
        self._check_func()

    def _check_func(self):
        try:
            exec(self._func_str)
            if self.func_des[2]:
                func_r = eval(self.func_des[2])
            else:
                func_r = eval(self.func_des[1])
            del func_r
            logger.debug("func_r={} checked by import and del with node name={}".format(self._func_str, self._name))
        except Exception as e:
            logger.error(e)
            err_msg = "func_r={} cannot imported with node_name={}".format(self._func_str, self._name)
            logger.error(err_msg)
            raise Exception(err_msg)

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, type_):
        if type_ != "cold":
            err_msg = "type={} now only support ['cold']".format(type_)
            logger.error(err_msg)
            raise Exception(err_msg)
        self._type = "cold"

    @property
    def inputs(self):
        return self._inputs

    @inputs.setter
    def inputs(self, inputs):
        if not isinstance(inputs, list) or not inputs or any(not isinstance(i, str) for i in inputs):
            err_msg = "inputs={} must be a real str list with node name={}".format(inputs, self._name)
            logger.error(err_msg)
            raise Exception(err_msg)
        self._inputs = inputs

    @property
    def outputs(self):
        return self._outputs

    @outputs.setter
    def outputs(self, outputs):
        if not isinstance(outputs, list) or not outputs or any(not isinstance(i, str) for i in outputs):
            err_msg = "outputs={} must be a real list with node name={}".format(outputs, self._name)
            logger.error(err_msg)
            raise Exception(err_msg)
        self._outputs = outputs

    @property
    def extra_args(self):
        return self._extra_args

    @property
    def extra_kwargs(self):
        return self._extra_kwargs

    @property
    def next_nodes(self):
        return self._next_nodes

    @next_nodes.setter
    def next_nodes(self, next_nodes):
        # next_nodes要求是列表，元素是str
        if not isinstance(next_nodes, list):
            err_msg = "next_nodes={} must be a list".format(next_nodes)
            logger.error(err_msg)
            raise Exception(err_msg)
        for name in next_nodes:
            if not isinstance(name, str):
                err_msg = "elements={} in next_nodes={} must str".format(name, next_nodes)
                logger.error(err_msg)
                raise Exception(err_msg)
                # break
        self._next_nodes = next_nodes

    @property
    def prep_nodes(self):
        return self._prep_nodes

    @prep_nodes.setter
    def prep_nodes(self, prep_nodes):
        # prep_nodes要求是列表，元素是str
        if not isinstance(prep_nodes, list):
            err_msg = "prep_nodes={} must be a list".format(prep_nodes)
            logger.error(err_msg)
            raise Exception(err_msg)
        for name in prep_nodes:
            if not isinstance(name, str):
                err_msg = "elements={} in prep_nodes={} must str".format(name, prep_nodes)
                logger.error(err_msg)
                raise Exception(err_msg)
                # break
        self._prep_nodes = prep_nodes

    @property
    def flags(self):
        return self._flags

    @flags.setter
    def flags(self, flags):
        # 给未来临时塞东西留个地方，目前没啥要求
        self._flags = flags

    @property
    def outputs_r(self):
        return self._outputs_r

    @outputs_r.setter
    def outputs_r(self, outputs_r):
        if not isinstance(outputs_r, dict):
            err_msg = "outputs_r={} must be a dict".format(outputs_r)
            logger.error(outputs_r)
            raise Exception(err_msg)
        # TODO：先注释掉这段保护，目前存的key是经过统一化的，而不是outputs里原始的样子
        # for k in outputs_r:
        #     if k not in self._outputs:
        #         err_msg = "key={} in outputs_r={} must in outputs={}".format(k, outputs_r, self._outputs)
        #         logger.error(err_msg)
        #         raise Exception(err_msg)
        self._outputs_r = outputs_r

    def _create_with_conf(self, conf):
        if not isinstance(conf, dict):
            err_msg = "pipenode conf={} must be a dict".format(conf)
            logger.error(err_msg)
            raise Exception(err_msg)
        self.name = conf.get("name")
        self.func_des = conf.get("func")
        self._set_func_str()  # self.func_str
        self.type = conf.get("type")
        self.inputs = conf.get("inputs")
        self.outputs = conf.get("outputs")
        # self.extra_args = None  # todo
        # self.extra_kwargs = None  # todo
        self.next_nodes = conf.get("next_nodes")
        self.prep_nodes = conf.get("prep_nodes")
        self.flags = conf.get("flags")
        self.outputs_r = {}

    def query(self, name="all"):
        name_list = ["all", "name", "func_des", "func_str", "type", "inputs", "outputs",
                     "next_nodes", "prep_nodes", "flags", "outputs_r"]
        if name not in name_list:
            rst = {"err_msg": "query name={} must in {}".format(name, name_list)}
            logger.debug("query pipenode={} with name={}".format(rst, name))
            return rst
        if name == "all":
            rst = {"name": self.name,
                   "func_des": self.func_des,
                   "func_str": self.func_str,
                   "type": self.type,
                   "inputs": self.inputs,
                   "outputs": self.outputs,
                   "next_nodes": self.next_nodes,
                   "prep_nodes": self.prep_nodes,
                   "outputs_r": self.outputs_r,
                   "flags": self.flags}
            logger.debug("query pipenode={} with name={}".format(rst, name))
            return rst
        else:
            rst = {name: eval("self.{}".format(name))}
            logger.debug("query pipenode={} with name={}".format(rst, name))
            return rst

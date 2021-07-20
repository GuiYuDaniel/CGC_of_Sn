# -*- coding:utf8 -*-
"""
pipenode负责构建单个计算节点：

1，节点信息

2，实例化函数

3，next方法等（本来有拓扑序不需要next方法了，这里是为了将来并行异步，做到形式上的统一）
"""

from utils.log import get_logger


logger = get_logger(__name__)


class Pipenode(object):
    """
    pipenode负责记录节点信息，由pipeline负责初始化。是计算到具体节点时，供pipetask访问的对象
    """

    def __init__(self, conf):
        self._name = None
        self._func_des = None
        self._func_str = None
        self._type = None
        self._inputs = None  # todo
        self._outputs = None  # todo
        self._extra_args = None  # todo
        self._extra_kwargs = None  # todo
        self._next_nodes = None
        self._prep_nodes = None
        self._flags = None
        self.real_outputs = {}

        self._create_with_conf(conf)
        self._check_func()

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
            raise err_msg
        self._name = name

    @property
    def func_des(self):
        return self._func_des

    @func_des.setter
    def func_des(self, func):  # func在ppnode里存描述，便于未来落盘需要
        # func要求是形式上确保将来可以form xx import yy as zz
        if not isinstance(func, list) or len(func) != 3 \
                or not isinstance(func[0], str) or not func[0].startwith("None") \
                or not isinstance(func[1], str) \
                or not isinstance(func[2], str):
            err_msg = "func={} must a len 3 list, and like ['None.xxx', 'yyy', 'zzz']" \
                      "in order to form xxx import yyy as zzz".format(func)
            logger.error(err_msg)
            raise err_msg
        self._func_des = func

    @property
    def func_str(self):
        return self._func_str

    @func_str.setter
    def func_str(self, top_path):
        if self.func_des[2]:
            func_str = "from {} import {} as {}".format(self.func_des[0], self.func_des[1], self.func_des[2])
        else:
            func_str = "from {} import {}".format(self.func_des[0], self.func_des[1])
        func_str = func_str.replace("None", top_path)
        self._func_str = func_str

    def _check_node(self):
        self._check_func()

    def _check_func(self):
        try:
            exec(self._func_str)
            if self.func_des[2]:
                func = eval(self.func_des[2])
            else:
                func = eval(self.func_des[1])
            del func
            logger.debug("func={} checked import and del".format(self._func_str))
        except Exception as e:
            logger.error(e)
            err_msg = "func={} cannot imported".format(self._func_str)
            logger.error(err_msg)
            raise err_msg

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, type_):
        if type_ != "cold":
            err_msg = "type={} now only support ['cold']".format(type_)
            logger.error(err_msg)
            raise err_msg
        self._type = "cold"

    @property
    def inputs(self):
        return self._inputs

    @property
    def outputs(self):
        return self._outputs

    @outputs.setter
    def outputs(self, outputs):
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
            raise err_msg
        for name in next_nodes:
            if not isinstance(name, str):
                err_msg = "elements={} in next_nodes={} must str".format(name, next_nodes)
                logger.error(err_msg)
                raise err_msg
                # break
        self._next_nodes = next_nodes

    @property
    def prep_nodes(self):
        return self._prep_nodes

    @prep_nodes.setter
    def prep_nodes(self, prep_nodes):
        # TODO 先按照在workflow里有的方式写，需要ppl那步，加到workflow里
        # prep_nodes要求是列表，元素是str
        if not isinstance(prep_nodes, list):
            err_msg = "prep_nodes={} must be a list".format(prep_nodes)
            logger.error(err_msg)
            raise err_msg
        for name in prep_nodes:
            if not isinstance(name, str):
                err_msg = "elements={} in prep_nodes={} must str".format(name, prep_nodes)
                logger.error(err_msg)
                raise err_msg
                # break
        self._prep_nodes = prep_nodes

    @property
    def flags(self):
        return self._flags

    @flags.setter
    def flags(self, flags):
        # 给未来临时塞东西留个地方，目前没啥要求
        self._flags = flags

    def _create_with_conf(self, conf):
        if not isinstance(conf, dict):
            err_msg = "pipenode conf={} must be a dict".format(conf)
            logger.error(err_msg)
            raise err_msg
        self.name = conf.get("name")
        self.func_des = conf.get("func")
        # self.func_str 在外部建立
        self.type = conf.get("type")
        # self.inputs = None  # todo
        self.outputs = conf.get("outputs")
        # self.extra_args = None  # todo
        # self.extra_kwargs = None  # todo
        self.next_nodes = conf.get("next_nodes")
        self.flags = conf.get("flags")

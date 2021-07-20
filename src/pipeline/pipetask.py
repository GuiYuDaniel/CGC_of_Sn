# -*- coding:utf8 -*-
"""
pipetask负责循环体调度：

起始作业
while循环：
    下一个node直到结束
"""

from utils.log import get_logger
from pipeline.pipeline import Pipeline


logger = get_logger(__name__)


class Pipetask(object):
    """
    真正干活儿的类
    """

    def __init__(self, ppl):
        if not isinstance(ppl, Pipeline):
            err_msg = "the type of ppl={} must pipeline".format(type(ppl).__name__)
            logger.error(err_msg)
            raise err_msg
        self._ppl = ppl
        self._finish_node_list = []

    def _simple_start(self):
        """这是一个按列表顺序执行的最简单的调度系统"""
        if not self._ppl.topo_order_list or not isinstance(self._ppl.topo_order_list, list):
            err_msg = "topo_order_list={} must be a real list".format(self._ppl.topo_order_list)
            logger.error(err_msg)
            raise err_msg
        logger.info("=" * 60)
        logger.info("pipeline will start with topo_order_list={}".format(self._ppl.topo_order_list))
        logger.info("=" * 60)
        for now_node_name in self._ppl.topo_order_list:
            logger.info("=" * 40)
            logger.info("pipenode={} start".format(now_node_name))
            logger.info("=" * 40)
            try:
                func = self._get_func(now_node_name)
                inputs = self._get_inputs(now_node_name)
                outputs = func(inputs)
                self._save_outputs_to_node(now_node_name, outputs)
                self._finish_node_list.append(now_node_name)
                self._save_pipetask()
            except Exception as e:
                logger.error(e)
                logger.error("pipeline error with node_name={} function={}".format(now_node_name, func.__name__))
                return "fail"
            logger.info("pipenode={} done with outputs={}".format(now_node_name, outputs))
            logger.info("pipenode={} end".format(now_node_name))
            logger.info("=" * 40)
        logger.info("=" * 60)
        logger.info("All pipeline done with finish_node_list={}".format(self._finish_node_list))
        logger.info("=" * 60)
        logger.info("打完收工！")
        return "success"

    def start(self):
        return self._simple_start()

    def restart(self):
        # 默认先试图restart，没有，则start
        pass

    def _get_func(self, node_name):
        # 获取当前node的function
        # 要不要检查下callable呢 把检查放在pipenode里了
        ppnode = self._ppl.node_dict.get(node_name)
        func_str = ppnode.func_str
        func_des = ppnode.func_des
        exec(func_str)
        if func_des[2]:
            func = eval(func_des[2])
        else:
            func = eval(func_des[1])
        return func

    def _get_inputs(self, node_name):
        # 获取当前函数的inputs
        ppnode = self._ppl.node_dict.get(node_name)
        inputs_name_list = ppnode.inputs
        prep_nodes = ppnode.prep_nodes
        if not prep_nodes:
            logger.debug("node={} is the first node".format(node_name))
            # TODO 还没想好第一个咋弄
        else:
            rst = []
            all_prep_outputs = {}
            for single_node in prep_nodes:
                prep_ppnode = self._ppl.node_dict.get(single_node)
                prep_outputs = prep_ppnode.real_outputs
                if prep_outputs:
                    all_prep_outputs.update(prep_outputs)
                else:
                    err_msg = "Cannot get prep_outputs for now_node={}, prep_node={}".format(node_name, single_node)
                    logger.error(err_msg)
                    return None
            for input_name in inputs_name_list:
                if input_name in all_prep_outputs:
                    rst.append(all_prep_outputs[input_name])
                else:
                    err_msg = "input_name={} not in all_prep_outputs={} for now_node={}, prep_node={}".format(
                        input_name, all_prep_outputs, node_name, prep_nodes)
                    logger.error(err_msg)
                    logger.debug("All prep_node_outputs will given bellow, and hope this will helpful")
                    for single_node in prep_nodes:
                        prep_ppnode = self._ppl.node_dict.get(single_node)
                        prep_outputs = prep_ppnode.outputs
                        prep_real_outputs = prep_ppnode.real_outputs
                        logger.debug("node={}, outputs={}, real_outputs={}".format(
                            single_node, prep_outputs, prep_real_outputs))
                    return None
            return tuple(rst)

    def _save_outputs_to_node(self, node_name, outputs):
        ppnode = self._ppl.node_dict.get(node_name)
        outputs_name_list = ppnode.outputs
        if len(outputs_name_list) != len(outputs):
            err_msg = "outputs={} must same len with outputs_name={}, \n" \
                      "node={} ".format(outputs, outputs_name_list, node_name)
            logger.error(err_msg)
            raise err_msg
        real_outputs_dict = {}
        for name, value in zip(outputs_name_list, outputs):
            real_outputs_dict[name] = value
        ppnode.real_outputs = real_outputs_dict

    def _save_pipetask(self):
        # 落有关pipe的盘，记录进度，帮助restart等行为的使用
        pass

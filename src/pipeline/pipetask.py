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
    TODO 未来如果需要引入微服务的话，过程中就要只抛错不raise了
    TODO inputs和outputs中的:::还是要在实际传入时去掉，主要是针对字典型!
    """

    def __init__(self, ppl):
        if not isinstance(ppl, Pipeline):
            err_msg = "the type of ppl={} must be pipeline".format(type(ppl).__name__)
            logger.error(err_msg)
            raise Exception(err_msg)
        self._ppl = ppl
        self._finish_node_list = []

    def _simple_start(self, *args):
        """这是一个按列表顺序执行的最简单的调度系统"""
        if not self._ppl.topo_order_list or not isinstance(self._ppl.topo_order_list, list):
            err_msg = "topo_order_list={} must be a not null list".format(self._ppl.topo_order_list)
            logger.error(err_msg)
            raise Exception(err_msg)
        logger.info("\n" + "=" * 80 + "\n" + "=" * 80 + "\n" + "=" * 80)
        logger.info("pipeline will start by topo_order_list={}".format(self._ppl.topo_order_list))
        logger.info("\n" + "=" * 80)
        for now_node_name in self._ppl.topo_order_list:
            logger.info("\n"+"=" * 40)
            logger.info("pipenode={} start".format(now_node_name))
            logger.info("\n"+"=" * 40)
            try:
                func_r = self._get_func_r(now_node_name)
                inputs_r = self._get_inputs_r(now_node_name, *args)
                outputs_r = func_r(*inputs_r)  # 注意，执行结果有可能很复杂，要尽量测试到所有情况
                self._save_outputs_to_node(now_node_name, outputs_r)
                self._finish_node_list.append(now_node_name)
                self._save_pipetask()
            except Exception as e:
                logger.error(e)
                logger.error("pipeline error with node_name={}, function={}".format(now_node_name, func_r.__name__))
                return "fail"
            logger.info("\n"+"=" * 40)
            logger.info("pipenode={} done with outputs={}".format(now_node_name, outputs_r))
            logger.info("pipenode={} succ".format(now_node_name))
            logger.info("now finish_node_list={}".format(self._finish_node_list))
            logger.info("\n"+"=" * 40)
        logger.info("\n" + "=" * 80 + "\n" + "=" * 80 + "\n" + "=" * 80)
        logger.info("All pipeline done with finish_node_list={}".format(self._finish_node_list))
        logger.info("\n" + "=" * 80)
        logger.info("打完收工！")
        return "success"

    def start(self, *args, mode="simple"):
        mode_list = ["simple"]
        if mode == "simple":
            return self._simple_start(*args)
        else:
            err_msg = "mode={} not in support mode_list={}".format(mode, mode_list)
            logger.error(err_msg)
            raise Exception(err_msg)

    def restart(self):
        # 默认先试图restart，没有，则start
        pass

    def _get_func_r(self, node_name):
        # 获取当前node的function
        ppnode = self._ppl.node_dict.get(node_name)
        func_str = ppnode.func_str
        func_des = ppnode.func_des
        exec(func_str)  # 把检查放在pipenode里了，在ppnode里没报错的话，这里肯定通过
        if func_des[2]:
            func_r = eval(func_des[2])
        else:
            func_r = eval(func_des[1])
        return func_r

    def _get_inputs_r(self, node_name, *args):
        # 获取当前函数的inputs
        # 起始节点返回输入的inputs_r，其他节点从outputs里找，要求都是函数式函数
        ppnode = self._ppl.node_dict.get(node_name)
        inputs_name_list = ppnode.inputs
        prep_nodes = ppnode.prep_nodes
        if not prep_nodes:
            logger.debug("node={} is the first node".format(node_name))
            # args本身就是tuple
            return args
        else:
            rst = []
            all_prep_outputs_r = {}
            for single_node in prep_nodes:
                prep_ppnode = self._ppl.node_dict.get(single_node)
                prep_outputs_r = prep_ppnode.outputs_r
                if prep_outputs_r:
                    all_prep_outputs_r.update(prep_outputs_r)
                else:
                    err_msg = "Cannot get prep_outputs_r for now_node={}, prep_node={}".format(node_name, single_node)
                    logger.error(err_msg)
                    return None
            for input_name in inputs_name_list:
                if input_name in all_prep_outputs_r:
                    rst.append(all_prep_outputs_r[input_name])
                else:
                    err_msg = "input_name={} not in all_prep_outputs_r={} for now_node={}, prep_node={}".format(
                        input_name, all_prep_outputs_r, node_name, prep_nodes)
                    logger.error(err_msg)
                    logger.debug("All prep_node_outputs_r will given bellow, and hope this will helpful")
                    for single_node in prep_nodes:
                        prep_ppnode = self._ppl.node_dict.get(single_node)
                        prep_outputs = prep_ppnode.outputs
                        prep_outputs_r = prep_ppnode.outputs_r
                        logger.debug("node={}, outputs={}, outputs_r={}".format(
                            single_node, prep_outputs, prep_outputs_r))
                    return None
            return tuple(rst)

    def _save_outputs_to_node(self, node_name, outputs_r):
        # 对应单输出和多输出，outputs可能是普通单变量或者tuple
        # 但是，单输出也有tuple的情况，要与多变量做区别
        ppnode = self._ppl.node_dict.get(node_name)
        outputs_name_list = ppnode.outputs
        # 因为python支持多return，无法根据函数的定义直接判断结果与conf里定义的是否一致，只能根据结果来判断
        if len(outputs_name_list) == 1:
            outputs_r_dict = {outputs_name_list[0]: outputs_r}
        else:
            if not isinstance(outputs_r, tuple):
                err_msg = "outputs_r={} must be tuple but it is {} when outputs multi with outputs_name={}, \n" \
                          "node={}".format(outputs_r, type(outputs_r), outputs_name_list, node_name)
                logger.error(err_msg)
                raise Exception(err_msg)
            if len(outputs_name_list) != len(outputs_r):
                err_msg = "outputs_r={} must same len with outputs_name={}, \n" \
                          "node={} ".format(outputs_r, outputs_name_list, node_name)
                logger.error(err_msg)
                raise Exception(err_msg)
            # TODO 目前的检查下，会漏一种情况，就是定义多输出的函数，实际是单输出tuple，且tuple内变量个数与定义的一致
            outputs_r_dict = {}
            for name, value in zip(outputs_name_list, outputs_r):
                outputs_r_dict[name] = value
        ppnode.outputs_r = outputs_r_dict

    def _save_pipetask(self):
        # 落有关pipe的盘，记录进度，帮助restart等行为的使用
        # 落盘pptask，就意味着ppl，ppn都要存了吧
        # restart有两个思路，一个是通过业务函数自行判断；另一个是通过pipeline先做粗粒度判断，业务函数在必要的地方加细粒度判断
        pass

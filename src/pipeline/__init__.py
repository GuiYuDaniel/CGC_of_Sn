# -*- coding:utf8 -*-
"""
pipeline负责作业的调度模块，
实现将DAG图指定的workflow，按照一定顺序（拓扑序）派发作业并执行的功能。

本模块目前是串行同步调度，视未来情况，可改为并行异步调度（celery，redis）

pipeline负责构建整体计算图
pipenode负责构建单个计算节点
pipetask负责循环体调度

调用时：
ppl = Pipeline(conf)  # 创建ppl
ppk = Pipetask(ppl)  # 创建ppk
rst = ppk.start(inputs_of_first_node)  # 执行
"""
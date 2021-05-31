# -*- coding:utf8 -*-
"""
模拟的core函数，用于测试pipeline，本质上这里的函数都应该有输入和输出
"""
import os


def where_am_i(normal, point_path=None):
    """
    :param normal:
    :param point_path:
    :return: the path of this function or point_path
    """
    if normal:
        if not point_path:
            path = os.getcwd()
        else:
            path = os.path.abspath(point_path)
    else:
        raise Exception('Fake Error')
    return path

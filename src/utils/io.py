# -*- coding: utf-8 -*-


import os
from functools import lru_cache
from utils.log import get_logger


logger = get_logger(__name__)


class Path(object):

    @classmethod
    def _get_full_path(cls, relative_path="", base_path_type="top"):
        """
        不开放这段代码了，将top，src，test的dirpath登记到Config里，使用时从conf拿更合适
        join the base_path and the relative_path, which base_path was auto fond
        不检查结果，因为外部使用exist调用很方便
        :param relative_path: should be a relative folder or file path(str)
        such as conf, conf/cgc_config.json
        :param base_path_type: [top, src, test](str)
        top means <CGC_of_Sn>,
        src means <CGC_of_Sn>/src,
        test means <CGC_of_Sn>/src
        :return: full path(str)
        """
        base_path_type_dict = {"top": "", "src": "src", "test": "test"}
        if not isinstance(relative_path, str):
            err_msg = "relative_path={} should be str".format(relative_path)
            logger.error(err_msg)
            raise err_msg
        if base_path_type not in list(base_path_type_dict.keys()):
            err_msg = "base_path_type={} should be in {}".format(base_path_type, list(base_path_type_dict.keys()))
            logger.error(err_msg)
            raise err_msg
        base_path = cls._get_top_path()
        full_path = os.path.join(base_path, base_path_type_dict.get(base_path_type), relative_path)
        return full_path

    @staticmethod
    @lru_cache()
    def _get_top_path():
        """
        guess from abspath
        :return: the path of <CGC_of_Sn>(str)
        """
        abs_path = os.path.abspath(__file__)  # <CGC_of_Sn>/src/utils/io.py
        if "src/utils/io.py" in abs_path:
            top_path = os.path.dirname(os.path.dirname(os.path.dirname(abs_path)))
            logger.debug("top path is {} lru cached".format(top_path))
        else:
            logger.warning('Cannot get top_path from abs_path={}'.format(abs_path))
            top_path = None
        return top_path

    def _check_top_path(self, python_path, abs_path):
        """

        :return:
        """
        pass


class Save(object):
    """
    save data
    """
    pass


class Load(object):
    """
    load data
    """
    pass

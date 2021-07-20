# -*- coding: utf-8 -*-


from utils.io import Path
from utils.json_op import Json


class Config(object):
    """
    A correct defined CGC config class,
    with cmd > user defined > default
    """

    def __init__(self):
        # TODO 这种不太希望外部调用时随意修改的属性，未来根据需要可以加上property装饰器
        # general conf
        self.name = None
        self.version = None
        self.github = None
        self.author = None
        self.license = None

        # user defined
        # TODO 把log实装到conf里
        # self.log_folder = None
        # self.log_level = None
        self.result_folder = None
        self.calc_target = None
        self.workflow_config = None

        # system conf
        self.top_path = Path._get_top_path()
        self.src_path = Path._get_full_path(base_path_type="src")
        self.test_path = Path._get_full_path(base_path_type="test")

        default_config = self._load_default_config()
        self._add_default_config(default_config)
        self._add_cmd_config()
        pass

    @staticmethod
    def _load_default_config():
        """Not all config param will be added"""
        default_config_path = Path._get_full_path(relative_path="conf/cgc_config.json", base_path_type="src")
        default_config = Json.file_to_json_without_comments(default_config_path)
        return default_config

    def _add_default_config(self, default_config):
        self.name = default_config.get("name")
        self.version = default_config.get("version")
        self.github = default_config.get("github")
        self.author = default_config.get("author")
        self.license = default_config.get("license")

        self.result_folder = default_config.get("DEFAULT_RESULT_FOLDER")
        self.calc_target = default_config.get("DEFAULT_CALC_TARGET")
        self.workflow_config = default_config.get("DEFAULT_WORKFLOW_FILE")
        pass

    def _add_cmd_config(self):
        pass



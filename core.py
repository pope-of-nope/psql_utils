from configparser import ConfigParser


class Config(object):
    config_file = "config.ini"

    @classmethod
    def get(cls):
        # type: ()->ConfigParser
        config = ConfigParser()
        with open(cls.config_file, 'r', encoding='utf-8') as f:
            return config.read_file(f)


class Environment(object):
    def __init__(self, config):
        # type: (ConfigParser)->None
        self.config = config

    def check(self):
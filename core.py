import json
import os
import shutil
from configparser import ConfigParser
import logging
from typing import Dict, List

logger = logging.Logger("psql_utils")
logger.setLevel(logging.DEBUG)


class Config(object):
    template_config = "config-template.ini"
    config_file = "config.ini"

    @classmethod
    def __ensure_config_file_exists(cls):
        if not os.path.isfile(cls.template_config):
            error = "Unable to locate template config file: %s" % cls.template_config
            logger.error(error)
            raise FileNotFoundError(error)
        if not os.path.isfile(cls.config_file):
            logger.info("Copying new config file '%s' from template config file '%s'" % (
                cls.config_file, cls.template_config
            ))
            try:
                shutil.copy(cls.template_config, cls.config_file)
            except Exception as e:
                logger.error(e)
                raise e
            else:
                logger.info("...successfully created config file.")

    @classmethod
    def __get(cls):
        # type: ()->ConfigParser
        cls.__ensure_config_file_exists()
        config = ConfigParser()
        with open(cls.config_file, 'r', encoding='utf-8') as f:
            return config.read_file(f)

    def __init__(self):
        # type: ()->None
        self.config = self.__get()


class PostgresUser(object):
    def __init__(self, username, password):
        # type: (str, str)->None
        self.username = username
        self.password = password

    def serialize(self):
        return {"username": self.username, "password": self.password}

    @classmethod
    def deserialize(cls, data):
        if isinstance(data, cls):
            return data
        elif isinstance(data, dict):
            return cls(**data)
        elif isinstance(data, str):
            return cls(**json.loads(data))
        elif isinstance(data, list):
            return [cls.deserialize(d) for d in data]


class Server(object):
    def __init__(self, name, version, host='localhost', port=5432, users=list()):
        # type: (str, str, str, int, List[PostgresUser])->None
        self.name = name
        self.version = version
        self.host = host
        self.port = port
        self.users = [PostgresUser.deserialize(u) for u in users]

    def serialize(self):
        return {"name": self.name, "version": self.version, "host": self.host, "port": self.port,
                "users": [u.serialize() for u in self.users]}

    @classmethod
    def deserialize(cls, data):
        if isinstance(data, cls):
            return data
        elif isinstance(data, dict):
            return cls(**data)
        elif isinstance(data, str):
            return cls(**json.loads(data))
        elif isinstance(data, list):
            return [cls.deserialize(d) for d in data]


class Manager(object):
    class Servers(object):
        _file = "servers.json"

        def _save(self):
            with open(self._file, 'w', encoding='utf8') as f:
                json.dump(self._servers, f)

        def _load(self):
            if not os.path.isfile(self._file):
                self._save()
            with open(self._file, 'r', encoding='utf8') as f:
                temp = json.load(f)
                for database_name, kwargs in temp.items():
                    self._servers[database_name] = Server(**kwargs)

        def __init__(self):
            self._servers = {}
            self._load()

        def __getitem__(self, database_name):
            # type: (str)->Server
            return self._servers[database_name]

    # class Users(object):
    #     _file = "users.json"
    #     def _save(self):
    #         with open(self._file, 'w', encoding='utf8') as f:
    #             json.dump(self._databases, f)
    #
    #     def _load(self):
    #         if not os.path.isfile(self._file):
    #             self._save()
    #         with open(self._file, 'r', encoding='utf8') as f:
    #             temp = json.load(f)
    #             for database_name, kwargs in temp.items():
    #                 self._databases[database_name] = Server(**kwargs)
    #     def __init__(self):
    #         self._users = {}


    # class PGPass(object):
    #     _file = "~/.pgpass"
    #     _backup = "~/.pgpass.backup"
    #
    #     def _ensure_backup_exists(self):
    #         if not os.path.isfile(self._backup):
    #
    #
    #     def _save(self):
#
# class PGPass(object):
#


class Interface(object):
    config = Config()

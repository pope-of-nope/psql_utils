import json
import os
import shutil
from configparser import ConfigParser
import logging
from typing import Dict, List, Generator

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


# class PostgresUser(object):
#     def __init__(self, username, password):
#         # type: (str, str)->None
#         self.username = username
#         self.password = password
#
#     def serialize(self):
#         return {"username": self.username, "password": self.password}
#
#     @classmethod
#     def deserialize(cls, data):
#         if isinstance(data, cls):
#             return data
#         elif isinstance(data, dict):
#             return cls(**data)
#         elif isinstance(data, str):
#             return cls(**json.loads(data))
#         elif isinstance(data, list):
#             return [cls.deserialize(d) for d in data]
#
#
# class Server(object):
#     def __init__(self, name, version, host='localhost', port=5432, users=list()):
#         # type: (str, str, str, int, List[PostgresUser])->None
#         self.name = name
#         self.version = version
#         self.host = host
#         self.port = port
#         self.users = [PostgresUser.deserialize(u) for u in users]
#
#     def serialize(self):
#         return {"name": self.name, "version": self.version, "host": self.host, "port": self.port,
#                 "users": [u.serialize() for u in self.users]}
#
#     @classmethod
#     def deserialize(cls, data):
#         if isinstance(data, cls):
#             return data
#         elif isinstance(data, dict):
#             return cls(**data)
#         elif isinstance(data, str):
#             return cls(**json.loads(data))
#         elif isinstance(data, list):
#             return [cls.deserialize(d) for d in data]


class PGPassEntry(object):
    def __init__(self, hostname, port, db, username, password):
        # type: (str, int, str, str, str)->None
        self.hostname = hostname
        self.port = port
        self.db = db
        self.username = username
        self.password = password

    @classmethod
    def from_line(cls, entry_line):
        # type: (str)->PGPassEntry
        args = entry_line.strip().split(":")
        hostname = args[0]
        port = int(args[1])
        db = args[2]
        username = args[3]
        password = args[4]
        return cls(hostname, port, db, username, password)

    def to_line(self):
        return ":".join([self.hostname, str(self.port), self.db, self.username, self.password])


class PGPassFile(object):
    _file = ".pgpass"

    def __init__(self, entries=list()):
        # type: (List[PGPassEntry])->None
        self.__entries = entries
        self.load()

    def load(self):
        if not os.path.isfile(self._file):
            self.save()
        with open(self._file, 'r', encoding='utf8') as f:
            self.__entries = [PGPassEntry.from_line(line) for line in f]

    def save(self):
        with open(self._file, 'w', encoding='utf8') as f:
            f.write("\n".join([e.to_line() for e in self.__entries]))

    def __iter__(self):
        # type: ()->Generator[PGPassEntry]
        for item in self.__entries:
            yield item


class Server(object):
    def __init__(self, name, version, host='localhost', port=5432):
        # type: (str, str, str, int)->None
        self.name = name
        self.version = version
        self.host = host
        self.port = port

    def serialize(self):
        return {"name": self.name, "version": self.version, "host": self.host, "port": self.port}

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
                json.dump([s.serialize() for s in self._servers], f)

        def _load(self):
            if not os.path.isfile(self._file):
                self._save()
            with open(self._file, 'r', encoding='utf8') as f:
                temp = json.load(f)
                self._servers = [Server.deserialize(s) for s in temp]

        def __init__(self, servers=list()):
            # type: (List[Server])->None
            self._servers = servers
            self._load()

        def __iter__(self):
            # type: ()->Generator[Server]
            for server in self._servers:
                yield server

        def __getitem__(self, server_name):
            # type: (str)->Server
            for s in self:
                if s.name == server_name:
                    return s
            raise KeyError(server_name)


class Interface(object):
    _servers = Manager.Servers()
    _credentials = PGPassFile()
    _config = Config()

    def __iter__(self):
        pass

    def select_server_prompt(self, retry=True):
        print("Listing servers:")
        servers = list(self._servers)

        for i, server in enumerate(servers):
            print("\t[%d.]: " % i, server.name)
        selection = input("Select a server (by name or number)")
        try:
            selected_index = int(selection)
            return servers[selected_index]
        except:
            selected_name = str(selection)
            for s in servers:
                if s.name == selected_name:
                    return s
            print("I didn't understand your selection: ", selection)
            if retry:
                self.select_server_prompt(retry=retry)
            else:
                raise KeyError(selection)


if __name__ == '__main__':
    manager = Interface()
    manager.select_server_prompt()

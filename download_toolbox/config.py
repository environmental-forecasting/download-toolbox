import datetime as dt
import logging
import os
import sys

from collections import UserDict

import orjson


class ConfigurationError(RuntimeError):
    pass


class Configuration(UserDict):
    def __init__(self,
                 directory,
                 identifier,
                 config_type,
                 **kwargs):
        super().__init__(kwargs)

        self._directory = directory
        self._identifier = identifier
        self._history = []
        self._config_type = config_type

        self._load_existing()

    def _load_existing(self):
        if os.path.exists(self.output_file):
            logging.info("Loading configuration {}".format(self.output_file))
            with open(self.output_file, "r") as fh:
                data = fh.read()
            obj = orjson.loads(data)
            self._history.extend(obj["history"])
            self.data.update(obj["data"])

    def render(self,
               owner,
               config_path=None,
               implementation=None):
        if config_path is not None:
            output_path = os.path.abspath(config_path)
            if not os.path.isdir(os.path.dirname(output_path)):
                raise ConfigurationError("Path {} should be path we can output to".format(config_path))
            self.directory = os.path.dirname(output_path)

        self._history.append(" ".join([
            "Run at {}: ".format(dt.datetime.now(dt.timezone.utc).strftime("%c %Z")),
            *sys.argv]))

        configuration = {
            "data": owner.get_config(),
            "history": self._history,
            "implementation": implementation
            if implementation is not None
            else ":".join([owner.__module__, owner.__class__.__name__]),
        }

        output_path = self.output_file if config_path is None \
            else os.path.join(config_path, os.path.split(self.output_file)[1]) if os.path.isdir(config_path) \
            else config_path

        logging.info("Writing configuration to {}".format(self.output_file))
        logging.debug(configuration)

        str_data = orjson.dumps(configuration, option=orjson.OPT_INDENT_2)
        with open(output_path, "w") as fh:
            fh.write(str_data.decode())
        return output_path

    @property
    def directory(self):
        return self._directory

    @directory.setter
    def directory(self, directory):
        if not os.path.isdir(directory):
            raise RuntimeError("Path {} is invalid, needs to be a directory".format(directory))
        self._directory = directory

    @property
    def identifier(self):
        return self._identifier

    @property
    def output_file(self):
        return os.path.join(self.directory, "{}.{}.json".format(self._config_type, self.identifier))

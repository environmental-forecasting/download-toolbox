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
                 identifier,
                 config_type: str = "config",
                 config_path: os.PathLike = None,
                 **kwargs):
        super().__init__(kwargs)

        self._identifier = identifier
        self._history = []
        self._path = config_path
        self._config_type = config_type

        self._load_existing()

    def _load_existing(self):
        if os.path.exists(self.output_path):
            logging.info("Loading configuration {}".format(self.output_path))
            with open(self.output_path, "r") as fh:
                data = fh.read()
            obj = orjson.loads(data)
            self._history.extend(obj["history"])
            self.data.update(obj["data"])

    def render(self,
               owner,
               implementation=None):
        if self._path is not None:
            output_path = os.path.abspath(self._path)
            if not os.path.isdir(os.path.dirname(output_path)):
                raise ConfigurationError("Path {} should be path we can output to".format(self._path))

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

        logging.info("Writing configuration to {}".format(self.output_path))
        str_data = orjson.dumps(configuration, option=orjson.OPT_INDENT_2)

        with open(self.output_path, "w") as fh:
            fh.write(str_data.decode())
        return self.output_path

    @property
    def identifier(self):
        return self._identifier

    @property
    def output_path(self):
        default_filename = "{}.{}.json".format(self._config_type, self.identifier)
        return os.path.join(".", default_filename) if self._path is None else \
            os.path.join(self._path, default_filename) if os.path.isdir(self._path) else \
            self._path

    @output_path.setter
    def output_path(self, path: os.PathLike) -> None:
        self._path = path

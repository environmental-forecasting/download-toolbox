import logging
import os

from collections import UserDict

import orjson


class ConfigurationError(RuntimeError):
    pass


class Configuration(UserDict):
    def __init__(self, directory, identifier, **kwargs):
        super().__init__(kwargs)

        self._directory = directory
        self._identifier = identifier
        self._history = []

        self._load_existing()

    def _load_existing(self):
        if os.path.exists(self.output_file):
            logging.info("Loading configuration {}".format(self.output_file))
            with open(self.output_file, "r") as fh:
                data = fh.read()
            obj = orjson.loads(data)
            self.data.update(obj["data"])

    def render(self, owner, directory=None):
        if directory is not None:
            if not os.path.isdir(directory):
                raise ConfigurationError("Path {} should be a directory".format(directory))
            self.directory = directory

        configuration = {
            "data": owner.get_config(),
            "history": self._history,
            "implementation": owner.__class__.__name__,
        }

        logging.info("Writing configuration to {}".format(self.output_file))
        logging.debug(configuration)

        str_data = orjson.dumps(configuration)
        with open(self.output_file, "w") as fh:
            fh.write(str_data.decode())
        return self.output_file

    @property
    def directory(self):
        return self._directory

    @property
    def identifier(self):
        return self._identifier

    @directory.setter
    def directory(self, directory):
        if not os.path.isdir(directory):
            raise RuntimeError("Path {} is invalid, needs to be a directory".format(directory))
        self._directory = directory

    @property
    def output_file(self):
        return os.path.join(self.directory, "download_toolbox.{}.json".format(self.identifier))

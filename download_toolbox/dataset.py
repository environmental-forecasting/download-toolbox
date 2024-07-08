import json
import logging
import os

import download_toolbox.data
from download_toolbox.location import Location


class DataSetFactory(object):
    @classmethod
    def get_item(cls, impl):
        klass_name = DataSetFactory.get_klass_name(impl)

        if hasattr(download_toolbox.data, klass_name):
            return getattr(download_toolbox.data, klass_name)

        logging.error("No class named {0} found in download_toolbox.data".format(klass_name))
        raise ReferenceError

    @classmethod
    def get_klass_name(cls, name):
        return name.split(":")[-1]


def get_dataset_implementation(config: os.PathLike):
    if not str(config).endswith(".json"):
        raise RuntimeError("{} does not look like a JSON configuration".format(config))
    if not os.path.exists(config):
        raise RuntimeError("{} is not a configuration in existence".format(config))

    logging.debug("Retrieving implementations details from {}".format(config))

    with open(config) as fh:
        cfg = json.load(fh)

    logging.debug("Attempting to instantiate {} with {}".format(cfg["implementation"], cfg["data"]))
    return DataSetFactory.get_item(cfg["implementation"]).open_config(cfg["data"])

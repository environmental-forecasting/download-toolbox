import logging
import os
import sys

import orjson

from download_toolbox.base import DataCollection
from download_toolbox.dataset import DatasetConfig
from download_toolbox.location import Location
from download_toolbox.time import Frequency

from download_toolbox.data.amsr import AMSRDatasetConfig
from download_toolbox.data.cds import ERA5DatasetConfig
from download_toolbox.data.esgf import CMIP6DatasetConfig
from download_toolbox.data.osisaf import SICDatasetConfig

__all__ = [
    "DataCollection",
    "DatasetConfig",
    "AMSRDatasetConfig",
    "ERA5DatasetConfig",
    "CMIP6DatasetConfig",
    "SICDatasetConfig",
    # Descriptions
    "Frequency",
    "Location",
    # Functions
    "get_dataset_config_implementation",
]


class DataSetFactory(object):
    @classmethod
    def get_item(cls, impl):
        klass_name = DataSetFactory.get_klass_name(impl)

        # This looks weird, but to avoid circular imports it helps to isolate implementations
        # herein, so that dependent libraries can more easily import functionality without
        # accidentally importing everything through download_toolbox.data
        if hasattr(sys.modules[__name__], klass_name):
            return getattr(sys.modules[__name__], klass_name)

        logging.error("No class named {0} found in download_toolbox.data".format(klass_name))
        raise ReferenceError

    @classmethod
    def get_klass_name(cls, name):
        return name.split(":")[-1]


def get_dataset_config_implementation(config: os.PathLike):
    if not str(config).endswith(".json"):
        raise RuntimeError("{} does not look like a JSON configuration".format(config))
    if not os.path.exists(config):
        raise RuntimeError("{} is not a configuration in existence".format(config))

    logging.debug("Retrieving implementations details from {}".format(config))

    with open(config) as fh:
        data = fh.read()

    cfg = orjson.loads(data)
    logging.debug("Loaded configuration {}".format(cfg))
    cfg, implementation = cfg["data"], cfg["implementation"]

    # TODO: Getting a nicer implementation might be the way forward, but this will do
    #  with the Frequency naively matching given that they're fully caps-locked strings
    location = Location(**cfg["_location"])
    freq_dict = {k.strip("_"): getattr(Frequency, v) for k, v in cfg.items() if v in list(Frequency.__members__)}
    remaining = {k.strip("_"): v
                 for k, v in cfg.items()
                 if k not in [*["_{}".format(el) for el in freq_dict.keys()], "_location", "_config_type"]}

    create_kwargs = dict(location=location, **remaining, **freq_dict)
    logging.info("Attempting to instantiate {} with loaded configuration".format(implementation))
    logging.debug("Converted kwargs from the retrieved configuration: {}".format(create_kwargs))

    return DataSetFactory.get_item(implementation)(**create_kwargs)

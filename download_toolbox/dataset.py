import json
import logging
import os
import tempfile
from dataclasses import dataclass
from pprint import pformat

import pandas as pd
import xarray as xr

import download_toolbox.data
from download_toolbox.base import DataCollection
from download_toolbox.config import Configuration
from download_toolbox.data.utils import merge_files
from download_toolbox.time import Frequency


@dataclass
class VarConfig:
    name: str
    prefix: str
    level: int
    path: os.PathLike
    root_path: os.PathLike


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


class DatasetConfig(DataCollection):
    """A datasetconfig is an implementation of the base data collection, adding characteristics.

    Yes, this is intentionally not called a dataset as it doesn't override xarray.Dataset and
    it feels nicer that it represents a configuration for a Dataset, rather than a Dataset itself

    The additional characteristics that are implemented at this level:
        1. Location awareness
        2. Variables and levels

    TODO: align to https://www.geoapi.org/snapshot/python/metadata.html#metadata-iso-19115
      - intention is to eventually describe the DatasetConfig and metadata conformantly

    :param overwrite: Flag specifying whether existing files should be overwritten or not.
    """

    def __init__(self,
                 *args,
                 frequency: object = Frequency.DAY,
                 levels: object = (),
                 location: object,
                 output_group_by: object = Frequency.YEAR,
                 overwrite: bool = False,
                 # TODO: Perhaps review the implementation with Enum to a bitwise typed one @ Py3.9+
                 valid_frequencies: tuple = (Frequency.DAY, Frequency.MONTH),
                 var_names: object = (),
                 **kwargs) -> None:
        super(DatasetConfig, self).__init__(*args,
                                            path_components=[frequency.name.lower(), location.name],
                                            **kwargs)

        self._frequency = frequency
        self._levels = list(levels)
        self._location = location
        self._output_group_by = output_group_by
        self._overwrite = overwrite
        self._var_names = list(var_names)

        if len(self._var_names) < 1:
            raise DataSetError("No variables requested")

        if len(self._levels) != len(self._var_names):
            raise DataSetError("# of levels must match # vars")

        if self._frequency < self._output_group_by:
            raise DataSetError("You can't request a higher output frequency than request frequency: {} vs {}".
                               format(self._output_group_by.name, self._frequency.name))

        if self._frequency not in valid_frequencies:
            raise DataSetError("Only the following frequencies are valid for request".format(valid_frequencies))

    def _get_data_var_folder(self,
                             var: str,
                             root: bool = False,
                             append: object = None,
                             missing_error: bool = False) -> str:
        """Returns the path for a specific data variable.

        Appends additional folders to the path if specified in the `append` parameter.

        :param var: The data variable.
        :param append: Additional folders to append to the path. Defaults to None.
        :param missing_error: Flag to specify if missing directories should be treated as an error. Defaults to False.
        :returns str: The path for the specific data variable.
        """
        if not append:
            append = []

        data_var_path = os.path.join(self.path if not root else self.root_path, *[var, *append])
        logging.debug("Handling data var path: {}".format(data_var_path))

        if not os.path.exists(data_var_path):
            if not missing_error:
                os.makedirs(data_var_path, exist_ok=True)
            else:
                raise OSError("Directory {} is missing and this is "
                              "flagged as an error!".format(data_var_path))

        return data_var_path

    def filter_extant_data(self,
                           var_config: VarConfig,
                           dates: list) -> list:
        dt_arr = list(reversed(sorted(dates)))
        filepaths = self.var_filepaths(var_config, dt_arr)

        # Filtering dates based on existing data
        extant_paths = set([filepath
                            for filepath in filepaths
                            if os.path.exists(filepath)])
        logging.info("Filtering {} dates against {} destination files".format(len(dt_arr), len(filepaths)))
        logging.debug("Filtering against: {}".format(pformat(filepaths)))

        if len(extant_paths) > 0:
            extant_ds = xr.open_mfdataset(extant_paths)
            exclude_dates = [pd.to_datetime(d).date() for d in extant_ds.time.values]

            dt_arr = sorted(list(set(dt_arr).difference(exclude_dates)))
            dt_arr.reverse()

            # We won't hold onto an active dataset during network I/O
            extant_ds.close()
            logging.debug("{} dates filtered down to {} dates".format(len(dates), len(dt_arr)))
        return dt_arr

    def save_data_for_config(self,
                             rename_var_list: dict = None,
                             source_ds: object = None,
                             source_files: list = None,
                             time_dim_values: list = None,
                             var_filter_list: list = None):
        # Check whether we have a valid source
        ds = None
        if type(source_ds) in [xr.Dataset, xr.DataArray]:
            ds = source_ds if type(source_ds) is xr.DataArray else source_ds.to_dataset()

            if source_files is not None:
                raise RuntimeError("Not able to combine sources in save_dataset at present")
        elif source_files is not None and len(source_files) > 0:
            try:
                logging.debug("Opening source files: {}".format(pformat(source_files)))
                ds = xr.open_mfdataset(source_files,
                                       combine="by_coords",
                                       parallel=True)
            except ValueError as e:
                logging.exception("Could not open files {} with error".format(source_files))
                raise DataSetError(e)

            if time_dim_values is not None:
                logging.warning("Assigning time dimension with {} values".format(len(time_dim_values)))
                ds = ds.assign(dict(time=[pd.Timestamp(d) for d in time_dim_values]))
        else:
            logging.warning("No data provided as data object or source files, not doing anything")
            if self._overwrite:
                logging.warning("Overwriting configuration even without data thanks to dataset.overwrite flag")
                self.save_config()
            return

        # Strip out unnecessary / unwanted variables
        if var_filter_list is not None:
            ds = ds.drop_vars(var_filter_list, errors="ignore")

        # TODO: Reduce spatially to required location

        # Reduce temporally to required resolution
        # TODO: Note, https://github.com/pydata/xarray/issues/364 for Grouper functionality?
        #   - we might have to roll our own functionality in the meantime, if necessary
        group_by = "time.{}".format(self._output_group_by.attribute)

        # Rename if requested
        if rename_var_list:
            logging.info("Renaming {} variables if available".format(rename_var_list))
            ds = ds.rename_vars({k: v for k, v in rename_var_list.items() if k in ds.data_vars})

        # For all variables in ds, determine if there are destinations available
        for var_config in [vc for vc in self.variables if vc.name in ds.data_vars]:
            da = getattr(ds, var_config.name)
            logging.debug("Resampling to period 1{}: {}".format(self.frequency.freq, da))
            da = da.resample(time="1{}".format(self.frequency.freq)).mean()

            logging.debug("Grouping {} by {}".format(var_config, group_by))
            for dt, dt_da in da.groupby(group_by):
                req_dates = pd.to_datetime(dt_da.time.values)
                logging.debug(req_dates)
                destination_path = self.var_filepath(var_config, req_dates)

                # If exists, merge and concatenate the data to destination (overwrite?) at output_group_by
                if os.path.exists(destination_path):
                    fh, temporary_name = tempfile.mkstemp(dir=".")
                    os.close(fh)
                    dt_da.to_netcdf(temporary_name)
                    dt_da.close()
                    logging.info("Written new data to {} and merging with {}".format(
                        temporary_name, destination_path
                    ))
                    merge_files(destination_path, temporary_name)
                else:
                    logging.info("Saving {}".format(destination_path))
                    dt_da.to_netcdf(destination_path)
                    dt_da.close()

        # Write out the configuration file
        self.save_config()

    def var_config(self, var_name, level=None):
        """

        :param var_name:
        :param level:
        :return:
        """
        var_full_name = "{}{}".format(var_name,
                                      str(level) if level is not None else "")

        return VarConfig(
            name=var_full_name,
            prefix=var_name,
            level=level,
            path=self._get_data_var_folder(var_full_name),
            root_path=self._get_data_var_folder(var_full_name, root=True)
        )

    def var_filepath(self, *args, **kwargs) -> os.PathLike:
        return self.var_filepaths(*args, **kwargs, single_only=True)[0]

    def var_filepaths(self,
                      var_config: VarConfig,
                      date_batch: list,
                      single_only: bool = False) -> list:
        """

        :param var_config:
        :param date_batch:
        :param single_only:
        :return:
        """
        output_filepaths = list(set([
            os.path.join(var_config.path, "{}.nc".format(date.strftime(self._output_group_by.date_format)))
            for date in date_batch]))

        if len(output_filepaths) > 1 and single_only:
            raise DataSetError("Filenames returned for {} dates should have been "
                               "singular but {} returnable, check your call / config".
                               format(len(date_batch), len(output_filepaths)))

        if len(output_filepaths) == 0:
            logging.warning("No filenames provided for {} - {}".format(var_config, len(date_batch)))

        # self.config.data["var_files"][var_config.name] = list(set(
        #     self.config.data["var_files"][var_config.name] + output_filepaths
        # ))

        return output_filepaths

    @property
    def config(self):
        if self._config is None:
            # TODO: this should not be always auto-generated - allow user specification
            config_ident = ".".join([self.frequency.name.lower(), self.location.name])

            logging.debug("Creating dataset configuration with {}".format(config_ident))
            self._config = Configuration(directory=self.root_path,
                                         identifier=config_ident)
        return self._config

    def get_config(self,
                   config_funcs: dict = None):
        my_funcs = dict(
            _frequency=lambda x: x.name,
            _location=lambda x: dict(name=x.name, bounds=x.bounds)
            if not x.north and not x.south else dict(name=x.name, north=x.north, south=x.south),
            _output_group_by=lambda x: x.name,
        )
        config_funcs = {} if config_funcs is None else config_funcs
        return super().get_config(config_funcs={**my_funcs, **config_funcs})

    @property
    def frequency(self):
        return self._frequency

    @property
    def location(self):
        return self._location

    @property
    def variables(self):
        for var_name, levels in zip(self._var_names, self._levels):
            for level in levels if levels is not None else [None]:
                var_config = self.var_config(var_name, level)
                # logging.debug("Returning configuration: {}".format(var_config))
                yield var_config


class DataSetError(RuntimeError):
    pass


import json
import logging
import os
import shutil
import tempfile
from dataclasses import dataclass
from pprint import pformat

import dask
import pandas as pd
import xarray as xr

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
                 *,
                 frequency: object = Frequency.DAY,
                 levels: object = (),
                 location: object,
                 output_group_by: object = Frequency.YEAR,
                 overwrite: bool = False,
                 path_components: list = None,
                 # TODO: Perhaps review the implementation with Enum to a bitwise typed one @ Py3.9+
                 valid_frequencies: tuple = (Frequency.DAY, Frequency.MONTH),
                 var_files: dict = None,
                 var_names: object = (),
                 **kwargs) -> None:
        super(DatasetConfig, self).__init__(config_type="dataset_config",
                                            path_components=[frequency.name.lower(), location.name]
                                            if path_components is None else path_components,
                                            **kwargs)

        self._frequency = frequency
        self._levels = list(levels)
        self._location = location
        self._output_group_by = output_group_by
        self._overwrite = overwrite
        self._var_files = dict() if var_files is None else var_files
        self._var_names = list(var_names)

        if len(self._var_names) < 1:
            raise DataSetError("No variables requested")

        if len(self._levels) != len(self._var_names):
            raise DataSetError("# of levels must match # vars")

        if self._frequency < self._output_group_by:
            raise DataSetError("You can't request a higher output frequency than request frequency: {} vs {}".
                               format(self._output_group_by.name, self._frequency.name))

        if self._frequency not in valid_frequencies:
            raise DataSetError("Only the following frequencies are valid for request: {}".format(valid_frequencies))

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
        # logging.debug("Handling {}data var path: {}".
        #               format("root " if root else "", data_var_path))

        if not os.path.exists(data_var_path):
            if not missing_error:
                os.makedirs(data_var_path, exist_ok=True)
            else:
                raise OSError("Directory {} is missing and this is "
                              "flagged as an error!".format(data_var_path))

        return data_var_path

    def copy_to(self,
                new_identifier: object,
                base_path: os.PathLike = None,
                skip_copy: bool = False) -> object:
        """

        Args:
            new_identifier:
            base_path:
            skip_copy:
        """
        old_path = self.path
        super().copy_to(new_identifier, base_path, skip_copy=True)
        logging.info("Applying copy_to to identifier {}".format(new_identifier))

        for var_name in self.var_files.keys():
            old_files = self.var_files[var_name]
            new_files = [var_file.replace(old_path, self.path) for var_file in old_files]

            for src, dest in zip(old_files, new_files):
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                logging.debug("Copying {} to {}".format(src, dest))
                shutil.copy(src, dest)

            self.var_files[var_name] = new_files

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

    def get_config(self,
                   config_funcs: dict = None,
                   strip_keys: list = None):
        my_keys = ["_overwrite"]

        def merge_var_files(x):
            data = dict() \
                if ("_var_files" not in self.config.data
                    or self.config.data["_var_files"] is None) \
                else self.config.data["_var_files"].copy()

            for var_name in x.keys():
                if var_name not in data:
                    data[var_name] = list()
                data[var_name].extend(x[var_name])
            return {k: list(sorted(set(files))) for k, files in data.items()}

        my_funcs = dict(
            _frequency=lambda x: x.name,
            _location=lambda x: dict(name=x.name, bounds=x.bounds)
            if not x.north and not x.south else dict(name=x.name, north=x.north, south=x.south),
            _output_group_by=lambda x: x.name,
            # TODO: this can't be done like this as the levels and var_names are ordered - GH#51
            # _var_names=lambda x: self._var_names + x,
            _var_files=merge_var_files
        )

        config_funcs = {} if config_funcs is None else config_funcs
        strip_keys = my_keys if strip_keys is None else my_keys + strip_keys
        return super().get_config(config_funcs={**my_funcs, **config_funcs},
                                  strip_keys=strip_keys)

    def get_dataset(self,
                    var_names: list = None):
        if var_names is None:
            logging.debug(self.variables)
            var_names = [v.name for v in self.variables]

        logging.debug("Finding files for {}".format(", ".join(var_names)))
        var_files = [var_filepaths
                     for vn in var_names
                     for var_filepaths in self.var_files[vn]]
        logging.info("Got {} filenames to open dataset with!".format(len(var_files)))
        logging.debug(pformat(var_files))

        # TODO: where's my parallel mfdataset please!?
        with dask.config.set(**{'array.slicing.split_large_chunks': True}):
            ds = xr.open_mfdataset(
                var_files,
                combine="nested",
                concat_dim="time",
                coords="minimal",
                compat="override"
            )

            ds = ds.drop_duplicates("time").chunk(dict(time=1, ))
        return ds

    def save_data_for_config(self,
                             combine_method: str = "by_coords",
                             rename_var_list: dict = None,
                             source_ds: object = None,
                             source_files: list = None,
                             time_dim_values: list = None,
                             var_filter_list: list = None):
        # Check whether we have a valid source
        ds = None
        if type(source_ds) in [xr.Dataset, xr.DataArray]:
            ds = source_ds if type(source_ds) is xr.Dataset else source_ds.to_dataset()

            if source_files is not None:
                raise RuntimeError("Not able to combine sources in save_dataset at present")
        elif source_files is not None and len(source_files) > 0:
            try:
                logging.debug("Opening source files: {}".format(pformat(source_files)))
                ds = xr.open_mfdataset(source_files,
                                       combine=combine_method,
                                       concat_dim=None if combine_method == "by_coords" else "time",
                                       parallel=True,
                                       engine="h5netcdf",
                                       )
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
        #  this will also need to set our shape details

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
            logging.debug("Resampling to period 1{}: {}".format(self.frequency.freq, da.shape))
            da = da.sortby("time").resample(time="1{}".format(self.frequency.freq)).mean(keep_attrs=True)

            logging.debug("Grouping {} by {}".format(var_config, group_by))
            for dt, dt_da in da.groupby(group_by):
                req_dates = pd.to_datetime(dt_da.time.values)
                logging.debug("Have group of {} dates".format(len(req_dates)))
                destination_path = self.var_filepath(var_config, req_dates)

                copy_attrs = {k: v for k, v in ds.attrs.items() if k.startswith("geospatial")}
                logging.debug("Reassinging geospatial info to derived dataset: {}".format(copy_attrs))
                dt_ds = dt_da.to_dataset().assign_attrs(copy_attrs)

                # If exists, merge and concatenate the data to destination (overwrite?) at output_group_by
                if os.path.exists(destination_path):
                    fh, temporary_name = tempfile.mkstemp(dir=".")
                    os.close(fh)
                    dt_ds.to_netcdf(temporary_name)
                    dt_ds.close()
                    logging.info("Written new data to {} and merging with {}".format(
                        temporary_name, destination_path
                    ))
                    merge_files(destination_path, temporary_name)
                else:
                    logging.info("Saving {}".format(destination_path))
                    dt_ds.to_netcdf(destination_path)
                    dt_ds.close()

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

        self._var_files[var_config.name] = list(set(
            self._var_files[var_config.name] + output_filepaths
        )) if var_config.name in self._var_files else output_filepaths

        return output_filepaths

    @property
    def config(self):
        if self._config is None:
            config_ident = ".".join(self.path_components)

            logging.debug("Creating dataset configuration with {}".format(config_ident))
            self._config = Configuration(config_type=self._config_type,
                                         directory=self.root_path,
                                         identifier=config_ident)
        return self._config

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

    @property
    def var_files(self):
        return self._var_files

    @var_files.setter
    def var_files(self, value: dict):
        logging.warning("Setting new file setup to dataset with {} files".format(
            ", ".join(["{} for {}".format(len(v), k) for k, v in value.items()])))
        self._var_files = value

    def __repr__(self):
        return pformat(self.__dict__)


class DataSetError(RuntimeError):
    pass


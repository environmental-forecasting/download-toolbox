import abc
from abc import abstractmethod, ABCMeta
import collections

import logging
import os
import tempfile

from download_toolbox.config import Configuration
from download_toolbox.location import Location
from download_toolbox.time import Frequency
from download_toolbox.data.utils import batch_requested_dates, merge_files

import pandas as pd
import xarray as xr


class DataCollection(metaclass=ABCMeta):
    """An Abstract base class with common interface for data collection classes.

    It represents a collection of data assets on a filesystem, though in the future
    it would make sense that we also allow use for object storage etc.

    This also handles automatic egress/ingress and validation of the configurations
    for these collections.

    :param _identifier: The identifier of the data collection.
    :param _path: The base path of the data collection.
    :raises AssertionError: Raised if identifier is not specified, or no hemispheres are selected.
    """

    @abstractmethod
    def __init__(self,
                 *args,
                 identifier: str,
                 path: str = os.path.join(".", "data"),
                 path_components: object = None,
                 **kwargs) -> None:
        self._identifier: str = identifier

        path_components = list() if path_components is None else path_components
        if not isinstance(path_components, list):
            raise DataCollectionError("path_components should be an Iterator")
        self._path = os.path.join(path, identifier, *path_components)
        self._root_path = os.path.join(path, identifier)

        if self._identifier is None:
            raise DataCollectionError("No identifier supplied")

        if os.path.exists(self._path):
            logging.debug("{} already exists".format(self._path))
        else:
            if not os.path.islink(self._path):
                logging.info("Creating path: {}".format(self._path))
                os.makedirs(self._path, exist_ok=True)
            else:
                logging.info("Skipping creation for symlink: {}".format(
                    self._path))

        self._config = Configuration(directory=self.base_path,
                                     identifier=self.identifier)

    @property
    def base_path(self) -> str:
        """The base path of the data collection."""
        return self._path

    @base_path.setter
    def base_path(self, path: str) -> None:
        self._path = path
        self._config.render(path)

    @property
    def config(self):
        return self._config

    @staticmethod
    def open_config(config):
        logging.info("Opening dataset config {}".format(config))
        raise RuntimeError("This is not yet implemented, get working for preprocess-toolbox!")

    @property
    def root_path(self):
        return self._root_path

    def save_config(self):
        saved_config = self._config.render(self)
        logging.info("Saved dataset config {}".format(saved_config))

    @property
    def identifier(self) -> str:
        """The identifier (label) for this data collection."""
        return self._identifier


class VarConfig(collections.namedtuple("VarConfig", ["name", "prefix", "level", "path", "root_path"])):
    def __repr__(self):
        return "{} with path {}".format(self.name, self.path)


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

        data_var_path = os.path.join(self.base_path if not root else self.root_path,
                                     *[var, *append])
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

        # Filtering dates based on existing data
        extant_paths = set([filepath
                            for filepath in self.var_filepaths(var_config, dt_arr)
                            if os.path.exists(filepath)])

        if len(extant_paths) > 0:
            extant_ds = xr.open_mfdataset(extant_paths)
            exclude_dates = pd.to_datetime(extant_ds.time.values)
            logging.info("Excluding {} dates already existing from {} dates "
                         "requested.".format(len(exclude_dates), len(dt_arr)))

            dt_arr = sorted(list(set(dt_arr).difference(exclude_dates)))
            dt_arr.reverse()

            # We won't hold onto an active dataset during network I/O
            extant_ds.close()
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
                ds = xr.open_mfdataset(source_files,
                                       concat_dim="time",
                                       combine="nested",
                                       parallel=True)
            except ValueError as e:
                logging.exception("Could not open files {} with error".format(source_files))
                raise DataSetError(e)

            if time_dim_values is not None:
                logging.warning("Assigning time dimension with {} values".format(len(time_dim_values)))
                ds = ds.assign(dict(time=[pd.Timestamp(d) for d in time_dim_values]))

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
        for var_name in [vn for vn in ds.data_vars if vn in self._var_names]:
            da = getattr(ds, var_name)
            logging.debug("Resampling to period 1{}".format(self.frequency.freq))
            da = da.resample(time="1{}".format(self.frequency.freq)).mean()

            levels = self._levels[self._var_names.index(var_name)]
            for level in levels if levels is not None else [None]:
                var_config = self.var_config(var_name, level)

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
        output_filepaths = set([
            os.path.join(var_config.path, "{}.nc".format(date.strftime(self._output_group_by.date_format)))
            for date in date_batch])

        if len(output_filepaths) > 1 and single_only:
            raise DataSetError("Filenames returned for {} dates should have been "
                               "singular but {} returnable, check your call / config".
                               format(len(date_batch), len(output_filepaths)))

        if len(output_filepaths) == 0:
            logging.warning("No filenames provided for {} - {}".format(var_config, len(date_batch)))
        else:
            logging.debug("Got filenames: {}".format(output_filepaths))
        return list(output_filepaths)

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
                logging.debug("Returning configuration: {}".format(var_config))
                yield var_config


class Downloader(metaclass=abc.ABCMeta):
    """Abstract base class for a downloader.

    Performs operations on DataSets, we handle operations affecting the status of
    said DatasetConfig:
        1. Specify date range

    """

    def __init__(self,
                 dataset: DatasetConfig,
                 *args,
                 delete_tempfiles: bool = True,
                 download: bool = True,
                 drop_vars: list = None,
                 end_date: object,
                 postprocess: bool = True,
                 requests_group_by: object = Frequency.MONTH,
                 source_min_frequency: object = Frequency.DAY,
                 source_max_frequency: object = Frequency.DAY,
                 start_date: object,
                 **kwargs):
        super().__init__()

        self._batch_frequency = source_min_frequency if requests_group_by < source_min_frequency else \
            source_max_frequency if requests_group_by > source_max_frequency else requests_group_by
        # TODO: this needs to be moved into download_toolbox.time
        self._dates = [pd.to_datetime(date).date() for date in
                       pd.date_range(start_date, end_date, freq=self._batch_frequency.freq)]
        self._delete = delete_tempfiles
        self._download = download
        self._drop_vars = list() if drop_vars is None else drop_vars
        self._files_downloaded = []
        self._postprocess = postprocess
        self._requests_group_by = requests_group_by
        self._source_min_frequency = source_min_frequency
        self._source_max_frequency = source_max_frequency

        self._ds = dataset

        if not self._delete:
            logging.warning("!!! Deletions of temp files are switched off: be "
                            "careful with this, you need to manage your "
                            "files manually")

        self._download_method = self._single_download

    def download(self):
        """Implements a download for the given dataset

        This method handles download per var-"date batch" for the dataset
        """
        for var_config in self.dataset.variables:
            dates = self.dataset.filter_extant_data(var_config, self.dates)

            for req_date_batch in batch_requested_dates(dates=dates, attribute=self.batch_frequency.attribute):
                logging.info("Processing download for {} with {} dates".
                             format(var_config.name, len(req_date_batch)))
                files_downloaded = self._download_method(var_config, req_date_batch)
                logging.info("{} files downloaded".format(len(files_downloaded)))
                self._files_downloaded.extend(files_downloaded)

    @abstractmethod
    def _single_download(self,
                         var_config: object,
                         req_dates: object) -> list:
        raise NotImplementedError("_single_download needs an implementation")

    @property
    def batch_frequency(self) -> Frequency:
        return self._batch_frequency

    @property
    def dataset(self):
        return self._ds

    @property
    def dates(self):
        return self._dates

    @property
    def delete(self):
        return self._delete

    @property
    def download_method(self) -> callable:
        if not self._download_method:
            raise RuntimeError("Downloader has no method set, "
                               "implementation error")
        return self._download_method

    @download_method.setter
    def download_method(self, method: callable):
        logging.debug("Setting download_method to {}".format(method))
        self._download_method = method

    @property
    def drop_vars(self):
        return self._drop_vars

    @property
    def files_downloaded(self):
        return self._files_downloaded

    @property
    def requests_group_by(self):
        return self._requests_group_by


class DataCollectionError(RuntimeError):
    pass


class DataSetError(RuntimeError):
    pass


class DownloaderError(RuntimeError):
    pass


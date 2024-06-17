import abc
from abc import abstractmethod, ABCMeta
import collections

import logging
import os

from download_toolbox.config import Configuration
from download_toolbox.location import Location
from download_toolbox.time import Frequency
from download_toolbox.data.utils import batch_requested_dates

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
            raise RuntimeError("path_components should be an Iterator")
        self._path = os.path.join(path, identifier, *path_components)

        assert self._identifier, "No identifier supplied"

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
    def identifier(self) -> str:
        """The identifier (label) for this data collection."""
        return self._identifier


class VarConfig(collections.namedtuple("VarConfig", ["name", "prefix", "level", "path"])):
    def __repr__(self):
        return "{} with path {}".format(self.name, self.path)


class DataSet(DataCollection):
    """A dataset is an implementation of the base collection, adding characteristics.

    The additional characteristics that are implemented at this level:
        1. Location awareness
        2. Variables and levels

    TODO: align to https://www.geoapi.org/snapshot/python/metadata.html#metadata-iso-19115
      - intention is to eventually describe the DataSet and metadata conformantly

    :param dry: Flag specifying whether the data producer should be in dry run mode or not.
    :param overwrite: Flag specifying whether existing files should be overwritten or not.
    """

    def __init__(self,
                 *args,
                 dry: bool = False,
                 frequency: object = Frequency.DAY,
                 levels: object = (),
                 location: object,
                 output_group_by: object = Frequency.YEAR,
                 overwrite: bool = False,
                 # TODO: Perhaps review the implementation with Enum to a bitwise typed one @ Py3.9+
                 valid_frequencies: tuple = (Frequency.DAY, Frequency.MONTH),
                 var_names: object = (),
                 **kwargs) -> None:
        super(DataSet, self).__init__(*args,
                                      path_components=[frequency.name.lower()],
                                      **kwargs)

        self._dry = dry
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
                               format(self._output_group_by, self._frequency))

        if self._frequency not in valid_frequencies:
            raise DataSetError("Only the following frequencies are valid for request".format(valid_frequencies))

    def _get_data_var_folder(self,
                             var: str,
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

        data_var_path = os.path.join(self.base_path,
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

    def var_config(self, var_name, level=None):
        var_full_name = "{}{}".format(var_name,
                                      str(level) if level is not None else "")

        return VarConfig(
            name=var_full_name,
            prefix=var_name,
            level=level,
            path=self._get_data_var_folder(var_full_name)
        )

    def var_filepaths(self,
                      var_config: VarConfig,
                      date_batch: list) -> set:
        output_filepaths = set([
            os.path.join(var_config.path, date.strftime(self._output_group_by.date_format))
            for date in date_batch])
        logging.debug("Got {} filenames".format(output_filepaths))
        return output_filepaths

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
    said DataSet:
        1. Specify date range

    """

    def __init__(self,
                 dataset: DataSet,
                 *args,
                 delete_tempfiles: bool = True,
                 download: bool = True,
                 drop_vars: list = None,
                 end_date: object,
                 postprocess: bool = True,
                 requests_group_by: object = Frequency.MONTH,
                 start_date: object,
                 **kwargs):
        super().__init__()

        # TODO: this needs to be moved into download_toolbox.time
        self._dates = [pd.to_datetime(date).date() for date in
                       pd.date_range(start_date, end_date, freq=dataset.frequency.freq)]
        self._delete = delete_tempfiles
        self._download = download
        self._drop_vars = list() if drop_vars is None else drop_vars
        self._files_downloaded = []
        self._postprocess = postprocess
        self._requests_group_by = requests_group_by

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

            for req_date_batch in batch_requested_dates(dates=dates, attribute=self.requests_group_by.attribute):
                logging.info("Processing download for {} with {} dates".
                             format(var_config.name, len(req_date_batch)))
                destination_file = self.dataset.var_filepaths(var_config, req_date_batch)

                if len(destination_file) > 1:
                    raise DownloaderError("The batch is too large for an individual file output: {}".
                                          format(destination_file))

                files_downloaded = self._download_method(var_config, req_date_batch, destination_file)
                logging.info("{} files downloaded".format(len(files_downloaded)))
                self._files_downloaded.extend(files_downloaded)

            # TODO: save_temporal_files needs to LOCK and merge data into existing files
            #destination_file = self.dataset.var_filepath(var_config, self.dates)
            #if self._postprocess:
            #    self.postprocess(temporary_files, destination_file)

    def postprocess(self, source_filename, destination_filename):
        logging.debug("Calling default postprocessor to move {} to {}".format(
            source_filename, destination_filename
        ))
        os.rename(source_filename, destination_filename)

    @abstractmethod
    def _single_download(self,
                         var_config: object,
                         req_dates: object,
                         download_path: object) -> list:
        raise NotImplementedError("_single_download needs an implementation")

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
    def output_group_by(self):
        return self._output_group_by

    @property
    def requests_group_by(self):
        return self._requests_group_by


class DataSetError(RuntimeError):
    pass


class DownloaderError(RuntimeError):
    pass


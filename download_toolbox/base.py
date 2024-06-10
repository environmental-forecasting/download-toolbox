import abc
from abc import abstractmethod, ABCMeta
from collections.abc import Iterator
import fnmatch

import ftplib
from ftplib import FTP
import requests

import logging
import os

from download_toolbox.config import Configuration
from download_toolbox.location import Location
from download_toolbox.time import DateRequest
from download_toolbox.data.utils import batch_requested_dates

import pandas as pd


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
        assert not isinstance(path_components, Iterator), "path_components should be an Iterator"
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
                 frequency: object = DateRequest.day,
                 levels: object = (),
                 location: object,
                 overwrite: bool = False,
                 var_names: object = (),
                 **kwargs) -> None:
        super(DataSet, self).__init__(*args,
                                      path_components=[frequency.value],
                                      **kwargs)

        self._dry = dry
        self._frequency = frequency
        self._levels = list(levels)
        self._location = location
        self._overwrite = overwrite
        self._var_names = list(var_names)

        assert len(self._var_names), "No variables requested"
        assert len(self._levels) == len(self._var_names), \
            "# of levels must match # vars"

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

    def filter_extant_data(self, var_config, dates):
        raise RuntimeError("Not yet implemented, do not use")

#         dt_arr = list(reversed(sorted(copy.copy(self._dates))))
#
#         # Filtering dates based on existing data
#         filter_years = sorted(set([d.year for d in dt_arr]))
#         extant_paths = [
#             os.path.join(self.get_data_var_folder(var),
#                          "{}.nc".format(filter_ds))
#             for filter_ds in filter_years
#         ]
#         extant_paths = [df for df in extant_paths if os.path.exists(df)]
#
#         if len(extant_paths) > 0:
#             extant_ds = xr.open_mfdataset(extant_paths)
#             exclude_dates = pd.to_datetime(extant_ds.time.values)
#             logging.info("Excluding {} dates already existing from {} dates "
#                          "requested.".format(len(exclude_dates), len(dt_arr)))
#
#             dt_arr = sorted(list(set(dt_arr).difference(exclude_dates)))
#             dt_arr.reverse()
#
#             # We won't hold onto an active dataset during network I/O
#             extant_ds.close()
#
#         # End filtering

    def var_config(self, var_name, level=None):
        var_full_name = "{}{}".format(var_name,
                                      str(level) if level is not None else "")
        return dict(
            name=var_full_name,
            prefix=var_name,
            level=level,
            path=self._get_data_var_folder(var_full_name)
        )

    @property
    def frequency(self):
        return self._frequency.value

    @property
    def location(self):
        return self._location

    @property
    def variables(self):
        for var_name, levels in zip(self._var_names, self._levels):
            for level in levels if levels is not None else [None]:
                var_config = self.var_config(var_name, level)
                logging.debug("Returning configuration: {}".
                              format(", ".join(var_config)))
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
                 requests_group_by: str = "month",
                 start_date: object,
                 **kwargs):
        super().__init__()

        # TODO: this needs to be moved into download_toolbox.time
        self._dates = [pd.to_datetime(date).date() for date in
                       pd.date_range(start_date, end_date, freq="D")]
        self._delete = delete_tempfiles
        self._download = download
        self._drop_vars = list() if drop_vars is None else drop_vars
        self._files_downloaded = []
        self._output_group_by = "year"
        self._output_date_format = "%Y"
        self._postprocess = postprocess
        self._requests_group_by = requests_group_by

        self._ds = dataset

        if not self._delete:
            logging.warning("!!! Deletions of temp files are switched off: be "
                            "careful with this, you need to manage your "
                            "files manually")

        self._download_method = self._single_download

    def download(self):
        """Implements a single download based on configured download_method

        This allows delegation of downloading logic in a consistent manner to
        the configured download_method, ensuring a guarantee of adherence to
        naming and processing flow within implementations.

        """
        for var_config in self._ds.variables:
            # TODO: we need to be filtering dates on DataSet in existence
            #  with individual downloaders managing whether they "re-download"
            #  dates = self.dataset.filter_extant_data(var_config, self.dates)

            for req_date_batch in batch_requested_dates(dates=self.dates, attribute=self.requests_group_by):
                logging.info("Processing single download for {} with {} dates".
                             format(var_config["name"], len(req_date_batch)))
                temporary_file, destination_file = \
                    self.get_download_filenames(var_config["path"], req_date_batch[0])
                self.download_method(var_config, req_date_batch, temporary_file)

                # TODO: save_temporal_files needs to merge data into existing files
                #  if self._postprocess:
                #      self.postprocess(temporary_file, destination_file)

    def get_download_filenames(self,
                               var_folder: str,
                               req_date: object,
                               date_format: str = None):
        """

        :param var_folder:
        :param req_date:
        :param date_format:
        :return:
        """

        filename_date = req_date.strftime(
            date_format if date_format is not None else self._output_date_format)

        preprocess_name = os.path.join(
            var_folder, "temp.{}.nc".format(filename_date))
        target_name = os.path.join(
            var_folder, "{}.nc".format(filename_date))

        logging.debug("Got filenames: {} and {}".format(preprocess_name, target_name))

        return preprocess_name, target_name

    def postprocess(self, source_filename, destination_filename):
        logging.debug("Calling default postprocessor to move {} to {}".format(
            source_filename, destination_filename
        ))
        os.rename(source_filename, destination_filename)

    def save_temporal_files(self, var_config, da, date_format=None, freq=None):
        """

        :param var_config:
        :param da:
        :param date_format:
        :param freq:
        """

        # TODO: Note, https://github.com/pydata/xarray/issues/364 for Grouper functionality?
        #   - we might have to roll our own functionality in the meantime, if necessary
        group_by = "time.{}".format(self.output_group_by) if not freq else freq

        for dt, dt_da in da.groupby(group_by):
            req_date = pd.to_datetime(dt_da.time.values[0])
            temporary_name, _ = \
                self.get_download_filenames(var_config["path"],
                                            req_date,
                                            date_format=date_format)

            logging.info("Retrieving and saving {}".format(temporary_name))
            dt_da.compute()
            dt_da.to_netcdf(temporary_name)
            self._files_downloaded.append(temporary_name)

    @abstractmethod
    def _single_download(self,
                         var_config: object,
                         req_dates: object,
                         download_path: object):
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


class FTPDownloader(Downloader):
    def __init__(self,
                 host: str,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self._ftp = None
        self._ftp_host = host
        self._cache = dict()

    def single_request(self,
                       source_dir: object,
                       source_filename: list,
                       destination_path: object):
        if self._ftp is None:
            logging.info("FTP opening")
            self._ftp = FTP(self._ftp_host)
            self._ftp.login()

        try:
            logging.debug("Changing to {}".format(source_dir))
            # self._ftp.cwd(source_dir)

            if source_dir not in self._cache:
                self._cache[source_dir] = self._ftp.nlst(source_dir)

            ftp_files = [el for el in self._cache[source_dir] if el.endswith(source_filename)]
            if not len(ftp_files):
                logging.warning("File is not available: {}".
                                format(source_filename))
                return None
        except ftplib.error_perm as e:
            logging.warning("FTP error, possibly missing directory {}: {}".format(source_dir, e))
            return None

        logging.debug("Attempting to retrieve to {} from {}".format(destination_path, ftp_files[0]))
        with open(destination_path, "wb") as fh:
            self._ftp.retrbinary("RETR {}".format(ftp_files[0]), fh.write)


class HTTPDownloader(Downloader):
    def __init__(self,
                 host: str,
                 *args,
                 source_base: object = None,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self._host = host
        self._source_base = source_base

    def single_request(self,
                       source: object,
                       destination_path: object,
                       method: str = "get",
                       request_options: dict = None):
        request_options = dict() if request_options is None else request_options
        source_url = "/".join([self._host, self._source_base, source])

        try:
            logging.debug("{}-ing {} with {}".format(method, source_url, request_options))
            response = getattr(requests, method)(source_url, **request_options)
        except requests.exceptions.RequestException as e:
            logging.warning("HTTP error, possibly missing directory {}: {}".format(source_url, e))
            return None

        logging.debug("Attempting to output response content to {}".format(destination_path))
        with open(destination_path, "wb") as fh:
            fh.write(response.content)


class DataSetError(RuntimeError):
    pass


class DownloaderError(RuntimeError):
    pass


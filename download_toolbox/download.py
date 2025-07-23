import abc
import concurrent
import logging
from abc import ABCMeta, abstractmethod
from typing import Union

from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from download_toolbox.data.utils import batch_requested_dates

from download_toolbox.dataset import DatasetConfig
from download_toolbox.time import Frequency

"""

"""


class Downloader(metaclass=abc.ABCMeta):
    """Abstract base class for a downloader.

    Performs operations on DataSets, we handle operations affecting the status of
    said DatasetConfig:
        1. Specify date range
        2. Specify batching behaviours:
            -

    """

    def __init__(self,
                 dataset: DatasetConfig,
                 *args,
                 batch_frequency: Union[Frequency, None] = None,
                 delete_tempfiles: bool = True,
                 download: bool = True,
                 drop_vars: list = None,
                 end_date: list,
                 postprocess: bool = True,
                 request_frequency: object = Frequency.MONTH,
                 source_min_frequency: object = Frequency.DAY,
                 source_max_frequency: object = Frequency.DAY,
                 start_date: list,
                 **kwargs):
        super().__init__()

        self._dates = [pd.to_datetime(date).date() for date in
                       pd.date_range(start_date, end_date, freq=dataset.frequency.freq)]

        self._delete = delete_tempfiles
        self._download = download
        self._drop_vars = list() if drop_vars is None else drop_vars
        self._files_downloaded = []
        # TODO: can and should (?) be populated as part of download - threaded==future-arg map
        self._missing_dates = []
        self._postprocess = postprocess
        self._request_frequency = source_min_frequency \
            if request_frequency < source_min_frequency else source_max_frequency \
            if request_frequency > source_max_frequency else request_frequency
        self._source_min_frequency = source_min_frequency
        self._source_max_frequency = source_max_frequency

        logging.info("Request frequency set to {}".format(self.request_frequency.name))

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

            for req_date_batch in batch_requested_dates(dates=dates, attribute=self.request_frequency.attribute):
                logging.info("Processing download for {} with {} dates".
                             format(var_config.name, len(req_date_batch)))
                files_downloaded = self._download_method(var_config, req_date_batch)

                if files_downloaded is not None:
                    logging.info("{} files downloaded".format(len(files_downloaded)))
                    self._files_downloaded.extend(files_downloaded)
                else:
                    logging.warning("Nothing downloaded for {} on batch {}".format(var_config.name, req_date_batch))

    @abstractmethod
    def _single_download(self,
                         var_config: object,
                         req_dates: object) -> list:
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
    def files_downloaded(self):
        return self._files_downloaded

    @property
    def missing_dates(self):
        return self._missing_dates

    @property
    def request_frequency(self) -> Frequency:
        return self._request_frequency

    @property
    def skipped_dates(self) -> set:
        return self._skipped_dates


class ThreadedDownloader(Downloader, metaclass=ABCMeta):
    """Data downloader base class for batching downloading

    :param dates:
    :param delete_tempfiles:
    :param download:
    :param group_dates_by:
    :param max_threads:
    :param postprocess:
    :param var_name_idx:
    """

    def __init__(self, *args,
                 max_threads: int = 1,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self._max_threads = max_threads

    def download(self):
        """Handles concurrent (threaded) downloading for variables

        This takes dates, variables and levels as configured, batches them into
        requests and submits those via a ThreadPoolExecutor for concurrent
        downloading. Returns nothing, relies on _single_download to implement
        appropriate updates to this object to record state changes arising from
        downloading.
        """

        logging.info("Building request(s), downloading and averaging "
                     "from {} API".format(self.dataset.identifier.upper()))

        req_list = list()

        for var_config in self.dataset.variables:
            dates = self.dataset.filter_extant_data(var_config, self.dates)

            for req_date_batch in batch_requested_dates(dates=dates, attribute=self.request_frequency.attribute):
                logging.info("Processing single download for {} with {} dates".
                             format(var_config.name, len(req_date_batch)))

                req_list.append((var_config, req_date_batch))

        max_workers = min(len(req_list), self._max_threads)

        if max_workers > 0:
            logging.info("Creating thread pool with {} workers to service {} batches"
                         .format(max_workers, len(req_list)))

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []

                for args in req_list:
                    future = executor.submit(self.download_method, *args)
                    futures.append(future)

                for future in concurrent.futures.as_completed(futures):
                    try:
                        files_downloaded = future.result()

                        if files_downloaded is not None:
                            logging.info("{} files downloaded".format(len(files_downloaded)))
                            self._files_downloaded.extend(files_downloaded)
                        else:
                            logging.warning("Nothing downloaded from threaded batch")

                    except Exception as e:
                        logging.exception("Thread failure: {}".format(e))

        logging.info("{} files downloaded".format(len(self._files_downloaded)))

    @property
    def max_threads(self):
        return self._max_threads


class DownloaderError(RuntimeError):
    pass

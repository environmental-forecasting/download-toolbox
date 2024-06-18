import concurrent
import logging
import threading
from abc import ABCMeta

from concurrent.futures import ThreadPoolExecutor

from download_toolbox.base import Downloader
from download_toolbox.data.utils import batch_requested_dates

import ftplib
from ftplib import FTP
import requests


"""

"""


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

        requests = list()

        for var_config in self.dataset.variables:
            for req_date_batch in batch_requested_dates(dates=self.dates, attribute=self.requests_group_by.attribute):
                logging.info("Processing single download for {} with {} dates".
                             format(var_config.name, len(req_date_batch)))

                requests.append((var_config, req_date_batch))

        max_workers = min(len(requests), self._max_threads)
        logging.info("Creating thread pool with {} workers to service {} batches"
                     .format(max_workers, len(requests)))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []

            for args in requests:
                future = executor.submit(self._single_download, *args)
                futures.append(future)

            for future in concurrent.futures.as_completed(futures):
                try:
                    self._files_downloaded.extend(future.result())
                except Exception as e:
                    logging.exception("Thread failure: {}".format(e))

        logging.info("{} files downloaded".format(len(self._files_downloaded)))


class FTPClient(object):
    def __init__(self,
                 host: str,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self._ftp = None
        self._ftp_host = host
        self._cache = dict()
        self._ftp_connections = dict()

    def single_request(self,
                       source_dir: object,
                       source_filename: list,
                       destination_path: object):
        thread_id = threading.get_native_id()
        if threading.get_native_id() not in self._ftp_connections:
            logging.debug("FTP opening for thread {}".format(thread_id))
            self._ftp_connections[thread_id] = FTP(self._ftp_host)
            ftp_connection = self._ftp_connections[thread_id]
            ftp_connection.login()
        else:
            ftp_connection = self._ftp_connections[thread_id]

        try:
            logging.debug("FTP changing to {}".format(source_dir))
            # self._ftp.cwd(source_dir)

            if source_dir not in self._cache:
                self._cache[source_dir] = ftp_connection.nlst(source_dir)

            ftp_files = [el for el in self._cache[source_dir] if el.endswith(source_filename)]
            if not len(ftp_files):
                logging.warning("File is not available: {}".
                                format(source_filename))
                return None
        except ftplib.error_perm as e:
            logging.warning("FTP error, possibly missing directory {}: {}".format(source_dir, e))
            return None

        logging.debug("FTP Attempting to retrieve to {} from {}".format(destination_path, ftp_files[0]))
        with open(destination_path, "wb") as fh:
            ftp_connection.retrbinary("RETR {}".format(ftp_files[0]), fh.write)


class HTTPClient(object):
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
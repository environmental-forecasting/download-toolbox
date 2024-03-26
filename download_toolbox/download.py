import concurrent
import logging
from abc import ABC

from concurrent.futures import ThreadPoolExecutor

from download_toolbox.base import Downloader
from download_toolbox.data.utils import \
    batch_requested_dates, filter_dates_on_data


"""

"""


class ThreadedDownloader(Downloader, ABC):
    """Data downloader base class for batching downloading

    The premise is

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
        # TODO: move        self._rotatable_files = []
        # TODO: move        self._sic_ease_cubes = dict()

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
            for req_date_batch in batch_requested_dates(dates=self.dates, attribute=self.requests_group_by):
                logging.info("Processing single download for {} with {} dates".
                             format(var_config["name"], len(req_date_batch)))
                temporary_file, destination_file = \
                    self.get_download_filenames(var_config["path"], req_date_batch[0])

                req_dates = filter_dates_on_data(temporary_file,
                                                 destination_file,
                                                 req_date_batch,
                                                 drop_vars=self.drop_vars)

                if len(req_dates) > 0:
                    requests.append((var_config, req_date_batch, temporary_file, destination_file))

        with ThreadPoolExecutor(max_workers=min(len(requests),
                                                self._max_threads)) \
                as executor:
            futures = []

            for args in requests:
                future = executor.submit(self._threaded_download, *args)
                futures.append(future)

            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.exception("Thread failure: {}".format(e))

        logging.info("{} daily files downloaded".
                     format(len(self._files_downloaded)))

    def _threaded_download(self,
                           var_config: object,
                           req_date_batch: object,
                           temporary_file: str,
                           destination_file: str):
        self.download_method(var_config, req_date_batch, temporary_file)

        if self._postprocess:
            self.postprocess(temporary_file, destination_file)

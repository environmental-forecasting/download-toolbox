import concurrent
import logging
import os
import re
import shutil
import tempfile

from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor
from itertools import product

from download_toolbox.base import Downloader
from download_toolbox.data.utils import assign_lat_lon_coord_system, \
    gridcell_angles_from_dim_coords, \
    invert_gridcell_angles, \
    rotate_grid_vectors
from download_toolbox.data.utils import \
    batch_requested_dates, filter_dates_on_data, merge_files
from download_toolbox.utils import run_command

import numpy as np
import pandas as pd
import xarray as xr

"""

"""




class ThreadedDownloader(Downloader):
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
                 drop_vars: list = None,
                 max_threads: int = 1,
                 **kwargs):
        super().__init__(*args, **kwargs)

# TODO: move        self._drop_vars = list() if drop_vars is None else drop_vars
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

        logging.info("Building request(s), downloading and daily averaging "
                     "from {} API".format(self.identifier.upper()))

        requests = list()

        for var_config in self._ds.variables:
            for req_date_batch in batch_requested_dates(dates=self.dates):
                logging.info("Processing single download for {} with {} dates".
                             format(var_config["name"], len(req_date_batch)))
                temporary_file, destination_file = \
                    self.get_download_filenames(var_config["path"], req_date_batch[0])

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
        self.postprocess(temporary_file, destination_file)

    # TODO: remove
    def _single_download(self,
                         var_config: object,
                         req_dates: object,
                         download_path: object):

        req_dates = filter_dates_on_data(latlon_path,
                                         regridded_name,
                                         req_dates,
                                         drop_vars=self._drop_vars)

        if len(req_dates):
            if self._download:
                with tempfile.TemporaryDirectory() as tmpdir:
                    tmp_latlon_path = os.path.join(tmpdir, os.path.basename("{}.download".format(latlon_path)))

                    self.download_method(var,
                                         level,
                                         req_dates,
                                         tmp_latlon_path)

                    if os.path.exists(latlon_path):
                        (ll_path, ll_file) = os.path.split(latlon_path)
                        rename_latlon_path = os.path.join(
                            ll_path, "{}_old{}".format(
                                *os.path.splitext(ll_file)))
                        os.rename(latlon_path, rename_latlon_path)
                        old_da = xr.open_dataarray(rename_latlon_path,
                                                   drop_variables=self._drop_vars)
                        tmp_da = xr.open_dataarray(tmp_latlon_path,
                                                   drop_variables=self._drop_vars)

                        logging.debug("Input (old): \n{}".format(old_da))
                        logging.debug("Input (dl): \n{}".format(tmp_da))

                        da = xr.concat([old_da, tmp_da], dim="time")
                        logging.debug("Output: \n{}".format(da))

                        da.to_netcdf(latlon_path)
                        old_da.close()
                        tmp_da.close()
                        os.unlink(rename_latlon_path)
                    else:
                        shutil.move(tmp_latlon_path, latlon_path)

                logging.info("Downloaded to {}".format(latlon_path))
            else:
                logging.info("Skipping actual download to {}".
                             format(latlon_path))
        else:
            logging.info("No requested dates remain, likely already present")

        if self._postprocess and os.path.exists(latlon_path):
            self.postprocess(var, latlon_path)

        if os.path.exists(latlon_path):
            self._files_downloaded.append(latlon_path)

    def postprocess(self, var, download_path):
        logging.debug("No postprocessing in place for {}: {}".
                      format(var, download_path))

    @property
    def delete(self):
        return self._delete

import copy
import fnmatch
import ftplib
import gzip
import logging
import os

import datetime as dt
import numpy as np
import pandas as pd
import xarray as xr

from download_toolbox.base import DatasetConfig, DataSetError, HTTPDownloader, DownloaderError
from download_toolbox.cli import download_args
from download_toolbox.download import ThreadedDownloader
from download_toolbox.location import Location
from download_toolbox.time import Frequency


var_remove_list = ["polar_stereographic", "land"]


# TODO: move resolutions elsewhere
class AMSRDatasetConfig(DatasetConfig):
    def __init__(self,
                 *args,
                 resolution=6.25,
                 **kwargs):
        # There are other resolutions available, this will need updating for more
        # products, such as 1km AMSR+MODIS
        if resolution not in (3.125, 6.25):
            raise DataSetError("{} is not a valid resolution".format(resolution))

        self._resolution = resolution

        super().__init__(*args,
                         identifier="amsr2_{:1.3f}".format(resolution).replace(".", ""),
                         var_names=["siconca"],
                         levels=[None],
                         **kwargs)

    @property
    def resolution(self):
        return self._resolution


class AMSRDownloader(HTTPDownloader):
    """Downloads AMSR2 SIC data from 2012-present using FTP.

    The data can come from yearly zips, or individual files

    We use the following for FTP downloads:
        - data.seaice.uni-bremen.de

    """
    def __init__(self,
                 dataset: AMSRDatasetConfig,
                 *args,
                 start_date: object,
                 **kwargs):
        amsr2_start = dt.date(2012, 7, 2)

        # TODO: Differing start date ranges for different products! Validate in dataset
        # TODO: In fact, all date filtering against existing data should be done via DatasetConfig
        if start_date < amsr2_start:
            raise DownloaderError("AMSR2 only exists past {}".format(amsr2_start))
        self._hemi_str = "s" if dataset.location.south else "n"

        super().__init__("https://data.seaice.uni-bremen.de",
                         dataset,
                         *args,
                         source_base="amsr2/asi_daygrid_swath/{}{}/netcdf".format(
                             self._hemi_str, "{:1.3f}".format(dataset.resolution).replace(".", "")),
                         start_date=start_date,
                         **kwargs)

    def _single_download(self,
                         var_config: object,
                         req_dates: object,
                         download_path: object):

        if len(set([el.year for el in req_dates]).difference([req_dates[0].year])) > 0:
            raise DownloaderError("Batches of dates must not exceed a year boundary for AMSR2")
        year_dir = str(req_dates[0].year)

        for file_date in req_dates:
            date_str = file_date.strftime("%Y%m%d")

            # amsr2/asi_daygrid_swath/s3125/netcdf/2017/asi-AMSR2-s3125-20170105-v5.4.nc
            # amsr2/asi_daygrid_swath/n6250/netcdf/2022/asi-AMSR2-n6250-20220103-v5.4.nc

            file_in_question = "{}/asi-AMSR2-{}{}-{}-v5.4.nc".\
                               format(year_dir, self._hemi_str, "{:1.3f}".format(self.dataset.resolution).replace(".", ""), date_str)
            destination_path = os.path.join(var_config.path
                                            , file_in_question)

            if not os.path.exists(os.path.dirname(destination_path)):
                os.makedirs(os.path.dirname(destination_path), exist_ok=True)

            if not os.path.exists(destination_path):
                try:
                    logging.info("Downloading {}".format(destination_path))
                    self.single_request(file_in_question, destination_path)
                except DownloaderError as e:
                    logging.warning("Failed to download {}: {}".format(destination_path, e))
            else:
                logging.debug("{} already exists".format(destination_path))


def main():
    args = download_args(var_specs=False,
                         workers=True)

    logging.info("AMSR-SIC Data Downloading")
    location = Location(
        name="hemi.{}".format(args.hemisphere),
        north=args.hemisphere == "north",
        south=args.hemisphere == "south",
    )

    dataset = AMSRDatasetConfig(
        location=location,
        # TODO: there is no frequency selection for raw data - aggregation is a
        #  concern of the process-toolbox
        frequency=getattr(Frequency, args.frequency),
    )

    sic = AMSRDownloader(
        dataset,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    sic.download()

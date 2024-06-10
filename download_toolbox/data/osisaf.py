import datetime as dt
import logging
import os

import pandas as pd

from download_toolbox.base import DataSet, DownloaderError, FTPDownloader, DataSetError
from download_toolbox.cli import download_args
from download_toolbox.download import ThreadedDownloader
from download_toolbox.location import Location
from download_toolbox.time import DateRequest

"""

"""

invalid_sic_days = {
    "north": [
        *[d.date() for d in
          pd.date_range(dt.date(1979, 5, 21), dt.date(1979, 6, 4))],
        *[d.date() for d in
          pd.date_range(dt.date(1979, 6, 10), dt.date(1979, 6, 26))],
        dt.date(1979, 7, 1),
        *[d.date() for d in
          pd.date_range(dt.date(1979, 7, 24), dt.date(1979, 7, 28))],
        *[d.date() for d in
          pd.date_range(dt.date(1980, 1, 4), dt.date(1980, 1, 10))],
        *[d.date() for d in
          pd.date_range(dt.date(1980, 2, 27), dt.date(1980, 3, 4))],
        *[d.date() for d in
          pd.date_range(dt.date(1980, 3, 16), dt.date(1980, 3, 22))],
        *[d.date() for d in
          pd.date_range(dt.date(1980, 4, 9), dt.date(1980, 4, 15))],
        *[d.date() for d in
          pd.date_range(dt.date(1981, 2, 27), dt.date(1981, 3, 5))],
        *[d.date() for d in
          pd.date_range(dt.date(1984, 8, 12), dt.date(1984, 8, 24))],
        dt.date(1984, 9, 14),
        *[d.date() for d in
          pd.date_range(dt.date(1985, 9, 22), dt.date(1985, 9, 28))],
        *[d.date() for d in
          pd.date_range(dt.date(1986, 3, 29), dt.date(1986, 7, 1))],
        *[d.date() for d in
          pd.date_range(dt.date(1987, 1, 3), dt.date(1987, 1, 19))],
        *[d.date() for d in
          pd.date_range(dt.date(1987, 1, 29), dt.date(1987, 2, 2))],
        dt.date(1987, 2, 23),
        *[d.date() for d in
          pd.date_range(dt.date(1987, 2, 26), dt.date(1987, 3, 2))],
        dt.date(1987, 3, 13),
        *[d.date() for d in
          pd.date_range(dt.date(1987, 3, 22), dt.date(1987, 3, 26))],
        *[d.date() for d in
          pd.date_range(dt.date(1987, 4, 3), dt.date(1987, 4, 17))],
        *[d.date() for d in
          pd.date_range(dt.date(1987, 12, 1), dt.date(1988, 1, 12))],
        dt.date(1989, 1, 3),
        *[d.date() for d in
          pd.date_range(dt.date(1990, 12, 21), dt.date(1990, 12, 26))],
        dt.date(1979, 5, 28),
        dt.date(1979, 5, 30),
        dt.date(1979, 6, 1),
        dt.date(1979, 6, 3),
        dt.date(1979, 6, 11),
        dt.date(1979, 6, 13),
        dt.date(1979, 6, 15),
        dt.date(1979, 6, 17),
        dt.date(1979, 6, 19),
        dt.date(1979, 6, 21),
        dt.date(1979, 6, 23),
        dt.date(1979, 6, 25),
        dt.date(1979, 7, 1),
        dt.date(1979, 7, 25),
        dt.date(1979, 7, 27),
        dt.date(1984, 9, 14),
        dt.date(1987, 1, 16),
        dt.date(1987, 1, 18),
        dt.date(1987, 1, 30),
        dt.date(1987, 2, 1),
        dt.date(1987, 2, 23),
        dt.date(1987, 2, 27),
        dt.date(1987, 3, 1),
        dt.date(1987, 3, 13),
        dt.date(1987, 3, 23),
        dt.date(1987, 3, 25),
        dt.date(1987, 4, 4),
        dt.date(1987, 4, 6),
        dt.date(1987, 4, 10),
        dt.date(1987, 4, 12),
        dt.date(1987, 4, 14),
        dt.date(1987, 4, 16),
        dt.date(1987, 4, 4),
        dt.date(1990, 1, 26),
        dt.date(2022, 11, 9),
    ],
    "south": [
        dt.date(1979, 2, 5),
        dt.date(1979, 2, 25),
        dt.date(1979, 3, 23),
        *[d.date() for d in
          pd.date_range(dt.date(1979, 3, 26), dt.date(1979, 3, 30))],
        dt.date(1979, 4, 12),
        dt.date(1979, 5, 16),
        *[d.date() for d in
          pd.date_range(dt.date(1979, 5, 21), dt.date(1979, 5, 27))],
        *[d.date() for d in
          pd.date_range(dt.date(1979, 7, 10), dt.date(1979, 7, 18))],
        dt.date(1979, 8, 10),
        dt.date(1979, 9, 3),
        *[d.date() for d in
          pd.date_range(dt.date(1980, 1, 4), dt.date(1980, 1, 10))],
        dt.date(1980, 2, 16),
        *[d.date() for d in
          pd.date_range(dt.date(1980, 2, 27), dt.date(1980, 3, 4))],
        *[d.date() for d in
          pd.date_range(dt.date(1980, 3, 14), dt.date(1980, 3, 22))],
        dt.date(1980, 3, 31),
        *[d.date() for d in
          pd.date_range(dt.date(1980, 4, 9), dt.date(1980, 4, 15))],
        dt.date(1980, 4, 22),
        *[d.date() for d in
          pd.date_range(dt.date(1981, 2, 27), dt.date(1981, 3, 5))],
        dt.date(1981, 6, 10),
        *[d.date() for d in
          pd.date_range(dt.date(1981, 8, 3), dt.date(1982, 8, 9))],
        dt.date(1982, 8, 6),
        *[d.date() for d in
          pd.date_range(dt.date(1983, 7, 7), dt.date(1983, 7, 11))],
        dt.date(1983, 7, 22),
        dt.date(1984, 6, 12),
        *[d.date() for d in
          pd.date_range(dt.date(1984, 8, 12), dt.date(1984, 8, 24))],
        *[d.date() for d in
          pd.date_range(dt.date(1984, 9, 13), dt.date(1984, 9, 17))],
        *[d.date() for d in
          pd.date_range(dt.date(1984, 10, 3), dt.date(1984, 10, 9))],
        *[d.date() for d in
          pd.date_range(dt.date(1984, 11, 18), dt.date(1984, 11, 22))],
        dt.date(1985, 7, 23),
        *[d.date() for d in
          pd.date_range(dt.date(1985, 9, 22), dt.date(1985, 9, 28))],
        *[d.date() for d in
          pd.date_range(dt.date(1986, 3, 29), dt.date(1986, 11, 2))],
        *[d.date() for d in
          pd.date_range(dt.date(1987, 1, 3), dt.date(1987, 1, 15))],
        *[d.date() for d in
          pd.date_range(dt.date(1987, 12, 1), dt.date(1988, 1, 12))],
        dt.date(1990, 8, 14),
        dt.date(1990, 8, 15),
        dt.date(1990, 8, 24),
        *[d.date() for d in
          pd.date_range(dt.date(1990, 12, 22), dt.date(1990, 12, 26))],
        dt.date(1979, 2, 5),
        dt.date(1979, 2, 25),
        dt.date(1979, 3, 23),
        dt.date(1979, 3, 27),
        dt.date(1979, 3, 29),
        dt.date(1979, 4, 12),
        dt.date(1979, 5, 16),
        dt.date(1979, 7, 11),
        dt.date(1979, 7, 13),
        dt.date(1979, 7, 15),
        dt.date(1979, 7, 17),
        dt.date(1979, 8, 10),
        dt.date(1979, 9, 3),
        dt.date(1980, 2, 16),
        dt.date(1980, 3, 15),
        dt.date(1980, 3, 31),
        dt.date(1980, 4, 22),
        dt.date(1981, 6, 10),
        dt.date(1982, 8, 6),
        dt.date(1983, 7, 8),
        dt.date(1983, 7, 10),
        dt.date(1983, 7, 22),
        dt.date(1984, 6, 12),
        dt.date(1984, 9, 14),
        dt.date(1984, 9, 16),
        dt.date(1984, 10, 4),
        dt.date(1984, 10, 6),
        dt.date(1984, 10, 8),
        dt.date(1984, 11, 19),
        dt.date(1984, 11, 21),
        dt.date(1985, 7, 23),
        *pd.date_range(dt.date(1986, 7, 2), dt.date(1986, 11, 1)),
        dt.date(1990, 8, 14),
        dt.date(1990, 8, 15),
        dt.date(1990, 8, 24),
        dt.date(2022, 11, 9),
    ]
}

var_remove_list = ['time_bnds', 'raw_ice_conc_values', 'total_standard_error',
                   'smearing_standard_error', 'algorithm_standard_error',
                   'status_flag', 'Lambert_Azimuthal_Grid']


class SICDataSet(DataSet):
    def __init__(self,
                 *args,
                 **kwargs):

        super().__init__(*args,
                         identifier="osisaf",
                         var_names=["siconca"],
                         levels=[None],
                         **kwargs)


class SICDownloader(FTPDownloader):
    """Downloads OSISAF SIC data from 2012-present using FTP.

    The data can come from yearly zips, or individual files

    We use the following for FTP downloads:
        - data.seaice.uni-bremen.de

    """
    def __init__(self,
                 dataset: SICDataSet,
                 *args,
                 start_date: object,
                 **kwargs):
        self._osi450_start = dt.date(1979, 1, 1)
        self._osi430b_start = dt.date(2016, 1, 1)

        # TODO: Differing start date ranges for different products! Validate in dataset
        # TODO: In fact, all date filtering against existing data should be done via DataSet
        if start_date < self._osi450_start:
            raise DownloaderError("OSISAF SIC only exists past {}".format(self._osi450_start))

        self._ftp_osi450 = "/reprocessed/ice/conc/v2p0/{:04d}/{:02d}/"
        self._ftp_osi430b = "/reprocessed/ice/conc-cont-reproc/v2p0/{:04d}/{:02d}/"
        ### v3p0 needs some inclusion? 2021 +
        # self._ftp_osi430b = "/reprocessed/ice/conc-cont-reproc/v3p0/{:04d}/{:02d}/"
        ### TODO: /reprocessed/ice/conc/v3p0/monthly/2020 - MONTHLIES
        ###  /reprocessed/ice/conc-cont-reproc/v3p0/monthly/2023

        self._version_str = "v2p0"
        self._source_base = self._ftp_osi450 if start_date < self._osi430b_start else self._ftp_osi430b

        if dataset.location.north:
            self._invalid_dates = invalid_sic_days["north"]
            self._hemi_str = "nh"
        elif dataset.location.south:
            self._invalid_dates = invalid_sic_days["south"]
            self._hemi_str = "sh"
        else:
            # TODO: other locations are valid, there is work to do to support their "cutting out"
            raise RuntimeError("Please only use this downloader with whole hemispheres")

        super().__init__("osisaf.met.no",
                         dataset,
                         *args,
                         start_date=start_date,
                         **kwargs)

    def _single_download(self,
                         var_config: object,
                         req_dates: object,
                         download_path: object):

        if len(set([el.year for el in req_dates]).difference([req_dates[0].year])) > 0:
            raise DownloaderError("Individual batches of dates must not exceed a year boundary for AMSR2")

        for file_date in req_dates:
            self._source_base = self._ftp_osi450 if file_date < self._osi430b_start else self._ftp_osi430b
            source_base = self._source_base.format(file_date.year, file_date.month)

            file_in_question = "ice_conc_{}_ease2-250_icdr-{}_{:04d}{:02d}{:02d}1200.nc". \
                format(self._hemi_str, self._version_str, file_date.year, file_date.month, file_date.day)

            destination_path = os.path.join(var_config["path"], file_in_question)

            if not os.path.exists(os.path.dirname(destination_path)):
                os.makedirs(os.path.dirname(destination_path), exist_ok=True)

            if not os.path.exists(destination_path):
                try:
                    logging.info("Downloading {}".format(destination_path))
                    self.single_request(source_base,
                                        file_in_question,
                                        destination_path)
                except DownloaderError as e:
                    logging.warning("Failed to download {}: {}".format(destination_path, e))
            else:
                logging.debug("{} already exists".format(destination_path))


def main():
    args = download_args(var_specs=False,
                         workers=True)

    logging.info("OSISAF-SIC Data Downloading")
    location = Location(
        name="hemi.{}".format(args.hemisphere),
        north=args.hemisphere == "north",
        south=args.hemisphere == "south",
    )

    dataset = SICDataSet(
        location=location,
        # TODO: there is no frequency selection for raw data - aggregation is a
        #  concern of the process-toolbox
        frequency=getattr(DateRequest, args.frequency),
    )

    sic = SICDownloader(
        dataset,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    sic.download()


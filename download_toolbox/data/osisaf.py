import datetime as dt
import logging
import os

import pandas as pd

from download_toolbox.dataset import DatasetConfig
from download_toolbox.cli import DownloadArgParser
from download_toolbox.download import ThreadedDownloader, DownloaderError
from download_toolbox.utils import FTPClient
from download_toolbox.location import Location
from download_toolbox.time import Frequency

"""

"""

"""
    https://osi-saf.eumetsat.int/community/list-of-service-messages/release-osi-saf-monthly-mean-sea-ice-concentration-cdricdr
"""
invalid_sic_months = {
    "north": [
        dt.date(1978, 10, 30),
        dt.date(1986, 4, 30),
        dt.date(1986, 5, 31),
        dt.date(1986, 6, 30),
        dt.date(1987, 12, 31),
    ],
    "south": [
        dt.date(1978, 10, 30),
        dt.date(1986, 4, 30),
        dt.date(1986, 5, 31),
        dt.date(1986, 6, 30),
        dt.date(1987, 12, 31),
    ]
}

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


class SICDatasetConfig(DatasetConfig):
    def __init__(self,
                 identifier=None,
                 levels=None,
                 var_names=None,
                 **kwargs):
        super().__init__(identifier="osisaf" if identifier is None else identifier,
                         levels=[None] if levels is None else levels,
                         var_names=["siconca"] if var_names is None else var_names,
                         **kwargs)

        invalid_dates = invalid_sic_days if self.frequency == Frequency.DAY else invalid_sic_months
        self._invalid_dates = invalid_dates["north" if self.location.north else "south"]

    @property
    def invalid_dates(self):
        return self._invalid_dates


class SICDownloader(ThreadedDownloader):
    """Downloads OSISAF SIC data from 1978-present using FTP.

    The data comes from individual files, daily or monthly, for this product:
    https://osi-saf.eumetsat.int/products/osi-450-a

    We use the following for FTP downloads:
        - osisaf.met.no

    """
    def __init__(self,
                 dataset: SICDatasetConfig,
                 *args,
                 start_date: object,
                 **kwargs):
        self._conc_start = dt.date(1978, 10, 25)
        self._reproc_start = dt.date(2021, 1, 1)

        if start_date < self._conc_start:
            raise DownloaderError("OSISAF SIC only exists past {}".format(self._conc_start))

        if not (dataset.location.north or dataset.location.south):
            # TODO: other locations are valid, there is work to do to support their "cutting out"
            raise RuntimeError("Please only use this downloader with whole hemispheres, for the mo")

        self._hemi_str = "nh" if dataset.location.north else "sh"

        self._ftp_client = FTPClient(host="osisaf.met.no")

        super().__init__(dataset,
                         *args,
                         # We can download both daily and monthly data
                         source_min_frequency=Frequency.MONTH,
                         start_date=start_date,
                         **kwargs)

    def _single_download(self,
                         var_config: object,
                         req_dates: object) -> list:

        downloaded_files = []

        for file_date in req_dates:
            monthly_file = self.dataset.frequency < Frequency.DAY

            if not monthly_file:
                version_str = "v3p0"
                freq_path_str = "{:04d}/{:02d}"
            else:
                version_str = "v3p0/monthly"
                freq_path_str = "{:04d}"

            ftp_conc = "/reprocessed/ice/conc/{}/{}/".format(version_str, freq_path_str)
            ftp_reproc = "/reprocessed/ice/conc-cont-reproc/{}/{}/".format(version_str, freq_path_str)

            if file_date < self._reproc_start:
                file_base = "ice_conc_{}_ease2-250_cdr-v3p0".format(self._hemi_str)
                source_base = ftp_conc
            else:
                file_base = "ice_conc_{}_ease2-250_icdr-v3p0".format(self._hemi_str)
                source_base = ftp_reproc

            source_base = source_base.format(file_date.year, file_date.month)

            if monthly_file:
                source_base = source_base.format(file_date.year)
                dest_base = freq_path_str.format(file_date.year)
                file_in_question = "{}_{:04d}{:02d}.nc". \
                    format(file_base, file_date.year, file_date.month)
            else:
                source_base = source_base.format(file_date.year, file_date.month)
                dest_base = freq_path_str.format(file_date.year, file_date.month)
                file_in_question = "{}_{:04d}{:02d}{:02d}1200.nc". \
                    format(file_base, file_date.year, file_date.month, file_date.day)

            destination_path = os.path.join(var_config.root_path, dest_base, file_in_question)

            if not os.path.exists(os.path.dirname(destination_path)):
                os.makedirs(os.path.dirname(destination_path), exist_ok=True)

            if not os.path.exists(destination_path):
                try:
                    logging.info("Downloading {}".format(destination_path))
                    self._ftp_client.single_request(source_base,
                                                    file_in_question,
                                                    destination_path)
                    downloaded_files.append(destination_path)
                except DownloaderError as e:
                    logging.warning("Failed to download {}: {}".format(destination_path, e))
                    self.missing_dates.append(file_date)
            else:
                logging.debug("{} already exists".format(destination_path))
                downloaded_files.append(destination_path)

        return downloaded_files


def main():
    args = DownloadArgParser().add_workers().parse_args()

    logging.info("OSISAF-SIC Data Downloading")
    location = Location(
        name=args.hemisphere,
        north=args.hemisphere == "north",
        south=args.hemisphere == "south",
    )

    dataset = SICDatasetConfig(
        location=location,
        frequency=getattr(Frequency, args.frequency),
        output_group_by=getattr(Frequency, args.output_group_by),
        config=args.config,
        overwrite=args.overwrite_config,
    )

    for start_date, end_date in zip(args.start_dates, args.end_dates):
        logging.info("Downloading between {} and {}".format(start_date, end_date))

        sic = SICDownloader(
            dataset,
            max_threads=args.workers,
            start_date=start_date,
            end_date=end_date,
        )
        sic.download()
        dataset.save_data_for_config(
            config_path=args.config,
            rename_var_list=dict(ice_conc="siconca"),
            source_files=sic.files_downloaded,
            var_filter_list=var_remove_list
        )


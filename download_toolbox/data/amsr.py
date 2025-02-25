import logging
import os

import datetime as dt

from download_toolbox.dataset import DatasetConfig, DataSetError
from download_toolbox.cli import DownloadArgParser
from download_toolbox.download import ThreadedDownloader, DownloaderError
from download_toolbox.utils import HTTPClient, ClientError
from download_toolbox.location import Location
from download_toolbox.time import Frequency


var_remove_list = []  # ["polar_stereographic", "land"]


class AMSRDatasetConfig(DatasetConfig):
    def __init__(self,
                 identifier=None,
                 levels=None,
                 resolution=6.25,
                 var_names=None,
                 **kwargs):
        # There are other resolutions available, this will need updating for more
        # products, such as 1km AMSR+MODIS
        if resolution not in (3.125, 6.25):
            raise DataSetError("{} is not a valid resolution".format(resolution))

        self._resolution = resolution

        super().__init__(identifier="amsr2_{:1.3f}".format(resolution).replace(".", "")
                         if identifier is None else identifier,
                         # TODO: see below, GH#10 on consistency of naming in datasets, this should be in IceNet
                         var_names=["siconca"] if var_names is None else var_names,
                         levels=[None] if levels is None else levels,
                         **kwargs)

    @property
    def resolution(self):
        return self._resolution


class AMSRDownloader(ThreadedDownloader):
    """Downloads AMSR2 SIC data from 2012-present using HTTPS.

    The data can come from yearly zips, or individual files. We target the individual files as in reality it's so much
    more sensible - the zips are not consistently avialable across the data ranges

    We use the following for HTTPS downloads:
        - https://data.seaice.uni-bremen.de

    """
    def __init__(self,
                 dataset: AMSRDatasetConfig,
                 *args,
                 start_date: object,
                 **kwargs):
        amsr2_start = dt.date(2012, 7, 2)

        # TODO: Differing start date ranges for different products! Validate in dataset
        if start_date < amsr2_start:
            raise DownloaderError("AMSR2 only exists past {}".format(amsr2_start))
        self._hemi_str = "s" if dataset.location.south else "n"
        self._http_client = HTTPClient("https://data.seaice.uni-bremen.de",
                                       source_base="amsr2/asi_daygrid_swath/{}{}/netcdf".format(
                                           self._hemi_str, "{:1.3f}".format(dataset.resolution).replace(".", "")))

        super().__init__(dataset,
                         *args,
                         start_date=start_date,
                         **kwargs)

    def _single_download(self,
                         var_config: object,
                         req_dates: object):

        files_downloaded = []

        for file_date in req_dates:
            year_dir = str(file_date.year)
            date_str = file_date.strftime("%Y%m%d")

            file_in_question = "{}/asi-AMSR2-{}{}-{}-v5.4.nc".\
                               format(year_dir, self._hemi_str, "{:1.3f}".
                                      format(self.dataset.resolution).
                                      replace(".", ""), date_str)
            destination_path = os.path.join(var_config.root_path, file_in_question)

            if not os.path.exists(os.path.dirname(destination_path)):
                os.makedirs(os.path.dirname(destination_path), exist_ok=True)

            if not os.path.exists(destination_path):
                try:
                    logging.info("Downloading {}".format(destination_path))
                    self._http_client.single_request(file_in_question, destination_path)
                    files_downloaded.append(destination_path)
                except (ClientError, DownloaderError) as e:
                    logging.warning("Failed to download {}: {}".format(destination_path, e))
                    self.missing_dates.append(file_date)
            else:
                logging.debug("{} already exists".format(destination_path))
                files_downloaded.append(destination_path)

        return files_downloaded


def main():
    args = DownloadArgParser().add_workers().add_extra_args([
        (["-r", "--resolution"], dict(
            type=float,
            choices=[3.125, 6.25],
            default=6.25
        ))]).parse_args()

    logging.info("AMSR-SIC Data Downloading")
    location = Location(
        name=args.hemisphere,
        north=args.hemisphere == "north",
        south=args.hemisphere == "south",
    )

    dataset = AMSRDatasetConfig(
        location=location,
        frequency=getattr(Frequency, args.frequency),
        output_group_by=getattr(Frequency, args.output_group_by),
        config_path=args.config,
        overwrite=args.overwrite_config,
    )

    for start_date, end_date in zip(args.start_dates, args.end_dates):
        logging.info("Downloading between {} and {}".format(start_date, end_date))
        sic = AMSRDownloader(
            dataset,
            max_threads=args.workers,
            start_date=start_date,
            end_date=end_date,
        )
        sic.download()
        dataset.save_data_for_config(
            combine_method="nested",
            # TODO: This should ideally be in IceNet? There is a bigger issue of naming to address (GH#10)
            rename_var_list=dict(z="siconca"),
            source_files=sic.files_downloaded,
            time_dim_values=[date for date in sic.dates if date not in sic.missing_dates],
            var_filter_list=var_remove_list
        )

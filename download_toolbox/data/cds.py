import datetime as dt
import logging
import requests
import requests.adapters
import os

import cdsapi as cds
import xarray as xr

from download_toolbox.base import DatasetConfig
from download_toolbox.cli import download_args
from download_toolbox.download import ThreadedDownloader, DownloaderError
from download_toolbox.location import Location
from download_toolbox.time import Frequency


class ERA5DatasetConfig(DatasetConfig):
    CDI_MAP = {
        'tas': '2m_temperature',
        'ta': 'temperature',  # 500
        'tos': 'sea_surface_temperature',
        'psl': 'surface_pressure',
        'zg': 'geopotential',  # 250 and 500
        'hus': 'specific_humidity',  # 1000
        'rlds': 'surface_thermal_radiation_downwards',
        'rsds': 'surface_solar_radiation_downwards',
        'uas': '10m_u_component_of_wind',
        'vas': '10m_v_component_of_wind',
    }

    def __init__(self, *args,
                 identifier=None,
                 cdi_map_override: object = None,
                 **kwargs):
        super().__init__(*args,
                         identifier="era5"
                         if identifier is None else identifier,
                         **kwargs)

        self._cdi_map = ERA5DatasetConfig.CDI_MAP
        if cdi_map_override is not None:
            # self._cdi_map = {k: cdi_map_override for k in ERA5DatasetConfig.CDI_MAP.keys()}
            self._cdi_map.update(cdi_map_override)

    @property
    def cdi_map(self):
        return self._cdi_map


class ERA5Downloader(ThreadedDownloader):
    def __init__(self,
                 dataset: ERA5DatasetConfig,
                 *args,
                 use_toolbox: bool = False,
                 show_progress: bool = False,
                 start_date: object,
                 **kwargs):
        era5_start = dt.date(1940, 1, 1)
        self.client = cds.Client(progress=show_progress)
        logging.getLogger("cdsapi").setLevel(logging.WARNING)

        self._use_toolbox = use_toolbox

        if start_date < era5_start:
            raise DownloaderError("{} is before the limited date for ERA5 of {}".
                                  format(start_date, era5_start))

        super().__init__(dataset,
                         *args,
                         source_min_frequency=Frequency.YEAR,
                         # TODO: validate handling of hourly data, but it is
                         #  possible as a temporal resolution
                         source_max_frequency=Frequency.HOUR,
                         start_date=start_date,
                         **kwargs)

        self.download_method = self._single_api_download
        if use_toolbox:
            self.download_method = self._single_toolbox_download

        if self.max_threads > 10:
            logging.info("Upping connection limit for max_threads > 10")
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=self.max_threads,
                pool_maxsize=self.max_threads
            )
            self.client.session.mount("https://", adapter)

    def _single_toolbox_download(self,
                                 var_config: object,
                                 req_dates: object) -> list:
        """Implements a single download from CDS Toolbox API

        :param var_config:
        :param req_dates: the request date
        """

        raise RuntimeError("Toolbox downloads are not yet implemented in download-toolbox")

    def _single_api_download(self,
                             var_config: object,
                             req_dates: object) -> list:
        """Implements a single download from CDS API

        :param var_config:
        :param req_dates: the request date
        """

        logging.debug("Processing {} dates for {}".format(len(req_dates), var_config))
        monthly_request = self.dataset.frequency < Frequency.DAY
        product_type = "reanalysis" if not monthly_request else "monthly_averaged_reanalysis_by_hour_of_day"
        temp_download_path = os.path.join(var_config.root_path,
                                          "temp.{}".format(os.path.basename(
                                              self.dataset.var_filepath(var_config, req_dates))))
        download_path = os.path.join(var_config.root_path,
                                     os.path.basename(self.dataset.var_filepath(var_config, req_dates)))

        retrieve_dict = {
            "product_type": product_type,
            "variable": self.dataset.cdi_map[var_config.prefix],
            "year": int(req_dates[0].year),
            "month": list(set(["{:02d}".format(rd.month)
                               for rd in sorted(req_dates)])),
            # TODO: assumption about the time of day!
            "time": "12:00",
            "format": "netcdf",
            "area": self.dataset.location.bounds,
        }

        level_id = "single-levels"
        if var_config.level:
            level_id = "pressure-levels"
            retrieve_dict["pressure_level"] = [var_config.level]
        dataset = "reanalysis-era5-{}{}".format(level_id, "-monthly-means" if monthly_request else "")

        if not monthly_request:
            retrieve_dict["day"] = ["{:02d}".format(d) for d in range(1, 32)]
            # retrieve_dict["time"] = ["{:02d}:00".format(h) for h in range(0, 24)]

        # TODO: we can merge_files into download_path to save on the caching
        if os.path.exists(temp_download_path):
            raise DownloaderError("{} already exists, this shouldn't be the case, please consider altering the "
                                  "time resolution of request to avoid downloaded data clashes".format(temp_download_path))

        try:
            logging.info("Downloading data for {}...".format(var_config.name))

            self.client.retrieve(
                dataset,
                retrieve_dict,
                temp_download_path)
            logging.info("Download completed: {}".format(temp_download_path))
        # cdsapi uses raise Exception in many places, so having a catch-all is appropriate
        except Exception as e:
            logging.exception("{} not downloaded, look at the problem".format(temp_download_path))
            self.missing_dates.extend(req_dates)
            return []

        ds = xr.open_dataset(temp_download_path)
        ds = ds.rename({list(ds.data_vars)[0]: var_config.name})
        ds.to_netcdf(download_path)
        ds.close()

        if os.path.exists(temp_download_path):
            logging.debug("Removing {}".format(temp_download_path))
            os.unlink(temp_download_path)
        return [download_path]

    def _single_download(self,
                         var_config: object,
                         req_dates: object) -> list:
        logging.warning("You're not going to get data by calling this! "
                        "Set download_method to an actual implementation.")


def main():
    args = download_args(choices=["cdsapi", "toolbox"],
                         # TODO: frequency
                         workers=True)

    logging.info("ERA5 Data Downloading")

    location = Location(
        name="hemi.{}".format(args.hemisphere),
        north=args.hemisphere == "north",
        south=args.hemisphere == "south",
    )

    dataset = ERA5DatasetConfig(
        levels=args.levels,
        location=location,
        var_names=args.vars,
        frequency=getattr(Frequency, args.frequency),
        output_group_by=getattr(Frequency, args.output_group_by),
    )

    era5 = ERA5Downloader(
        dataset,
        start_date=args.start_date,
        end_date=args.end_date,
        max_threads=args.workers,
        request_frequency=getattr(Frequency, args.output_group_by),
        use_toolbox=args.choice == "toolbox"
    )
    era5.download()

    dataset.save_data_for_config(
        source_files=era5.files_downloaded,
        var_filter_list=["lambert_azimuthal_equal_area"],
    )

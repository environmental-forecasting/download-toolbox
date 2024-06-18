import datetime as dt
import logging
import os
import requests
import requests.adapters

from pprint import pformat

import cdsapi as cds

from download_toolbox.base import DatasetConfig, Downloader
from download_toolbox.cli import download_args
from download_toolbox.download import ThreadedDownloader
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
                 *args,
                 use_toolbox: bool = False,
                 show_progress: bool = False,
                 **kwargs):
        super().__init__(*args,
                         drop_vars=["lambert_azimuthal_equal_area"],
                         **kwargs)
        self.client = cds.Client(progress=show_progress)

        self._use_toolbox = use_toolbox
        self.download_method = self._single_api_download

        if use_toolbox:
            self.download_method = self._single_toolbox_download

        if self._max_threads > 10:
            logging.info("Upping connection limit for max_threads > 10")
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=self._max_threads,
                pool_maxsize=self._max_threads
            )
            self.client.session.mount("https://", adapter)

    def _single_toolbox_download(self,
                                 var_config: object,
                                 req_dates: object,
                                 download_path: object):
        """Implements a single download from CDS Toolbox API

        :param var:
        :param level: the pressure level to download
        :param req_dates: the request dates
        :param download_path:
        """

        logging.debug("Processing {} dates".format(len(req_dates)))
        var_prefix, level = var_config.prefix, var_config.level

        params_dict = {
            "realm": "c3s",
            "project": "app-c3s-daily-era5-statistics",
            "version": "master",
            "workflow_name": "application",
            "kwargs": {
                "dataset": "reanalysis-era5-single-levels",
                "product_type": "reanalysis",
                "variable": self.dataset.cdi_map[var_prefix],
                "pressure_level": "-",
                "statistic": "daily_mean",
                "year": req_dates[0].year,
                "month": sorted(list(set([r.month for r in req_dates]))),
                "frequency": "1-hourly",
                "time_zone": "UTC+00:00",
                "grid": "0.25/0.25",
                "area": {
                    "lat": [min([self.dataset.location.bounds[0],
                                 self.dataset.location.bounds[2]]),
                            max([self.dataset.location.bounds[0],
                                 self.dataset.location.bounds[2]])],
                    "lon": [min([self.dataset.location.bounds[1],
                                 self.dataset.location.bounds[3]]),
                            max([self.dataset.location.bounds[1],
                                 self.dataset.location.bounds[3]])],
                },
            },
        }

        if level:
            params_dict["kwargs"]["dataset"] = \
                "reanalysis-era5-pressure-levels"
            params_dict["kwargs"]["pressure_level"] = level

        logging.debug("params_dict: {}".format(pformat(params_dict)))
        result = self.client.service(
            "tool.toolbox.orchestrator.workflow",
            params=params_dict)

        try:
            logging.info("Downloading data for {}...".format(var_config.name))
            logging.debug("Result: {}".format(result))

            location = result[0]['location']
            res = requests.get(location, stream=True)

            logging.info("Writing data to {}".format(download_path))

            with open(download_path, 'wb') as fh:
                for r in res.iter_content(chunk_size=1024):
                    fh.write(r)

            logging.info("Download completed: {}".format(download_path))

        except Exception as e:
            logging.exception("{} not deleted, look at the "
                              "problem".format(download_path))
            raise RuntimeError(e)

    def _single_api_download(self,
                             var_config: object,
                             req_dates: object,
                             download_path: object):
        """Implements a single download from CDS API

        :param var:
        :param level: the pressure level to download
        :param req_dates: the request date
        :param download_path:
        """

        logging.debug("Processing {} dates for {}".format(len(req_dates), var_config))
        var_prefix, level = var_config.prefix, var_config.level

        retrieve_dict = {
            # TODO: DAILY: "product_type": "reanalysis",
            "product_type": "monthly_averaged_reanalysis",
            "variable": self.dataset.cdi_map[var_prefix],
            "year": req_dates[0].year,
            "month": list(set(["{:02d}".format(rd.month)
                               for rd in sorted(req_dates)])),
            # TODO: IF DAILY
            # "day": ["{:02d}".format(d) for d in range(1, 32)],
            # "time": ["{:02d}:00".format(h) for h in range(0, 24)],
            # TODO: IF MONTHLY
            "time": "00:00",
            "format": "netcdf",
            "area": self.dataset.location.bounds,
        }

        # TODO: IF DAILY
        dataset = "reanalysis-era5-single-levels"
        if level:
            dataset = "reanalysis-era5-pressure-levels"
            retrieve_dict["pressure_level"] = level

        # TODO: IF MONTHLY
        dataset = "reanalysis-era5-single-levels-monthly-means"
        if level:
            dataset = "reanalysis-era5-pressure-levels-monthly-means"
            retrieve_dict["pressure_level"] = level

        try:
            logging.info("Downloading data for {}...".format(var_config.name))

            self.client.retrieve(dataset, retrieve_dict, download_path)
            logging.info("Download completed: {}".format(download_path))

        except Exception as e:
            logging.exception("{} not deleted, look at the "
                              "problem".format(download_path))
            raise RuntimeError(e)

    def _single_download(self,
                         var_config: object,
                         req_dates: object,
                         download_path: object):
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
    )

    era5 = ERA5Downloader(
        dataset=dataset,
        start_date=args.start_date,
        end_date=args.end_date,
        delete_tempfiles=args.delete,

        # TODO: this needs to be based on Frequency
        requests_group_by="year",
        max_threads=args.workers,
        use_toolbox=args.choice == "toolbox"
    )
    era5.download()

import boto3
import concurrent
import datetime as dt
import fsspec
import logging
import re
import requests
import requests.adapters
import os

import cdsapi as cds
import pandas as pd
import xarray as xr

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from pprint import pformat
from typing import Union
from warnings import warn

from botocore import UNSIGNED
from botocore.config import Config

from download_toolbox.cli import AWSDownloadArgParser, CDSDownloadArgParser, DownloadArgParser
from download_toolbox.dataset import DatasetConfig
from download_toolbox.data.utils import batch_requested_dates, xr_save_netcdf
from download_toolbox.download import ThreadedDownloader, DownloaderError
from download_toolbox.location import Location
from download_toolbox.time import Frequency


class CDSDatasetConfig(DatasetConfig):
    CDI_MAP = {
        'tas': '2m_temperature',
        'ta': 'temperature',  # 500
        'tos': 'sea_surface_temperature',
        'ps': 'surface_pressure',
        'zg': 'geopotential',  # 250 and 500
        'hus': 'specific_humidity',  # 1000
        'rlds': 'surface_thermal_radiation_downwards',
        'rsds': 'surface_solar_radiation_downwards',
        'uas': '10m_u_component_of_wind',
        'vas': '10m_v_component_of_wind',
        'ua': 'u_component_of_wind',
        'va': 'v_component_of_wind',
        'sic': 'sea_ice_cover',
        'psl': 'mean_sea_level_pressure',
    }

    def __init__(self,
                 identifier: str = None,
                 cdi_map: object = None,
                 **kwargs):
        super().__init__(identifier="cds"
                         if identifier is None else identifier,
                         **kwargs)

        self._cdi_map = CDSDatasetConfig.CDI_MAP
        if cdi_map is not None:
            self._cdi_map.update(cdi_map)

        for var_config in self.variables:
            if var_config.prefix not in self._cdi_map:
                raise RuntimeError("{} requested but we don't have a map to CDS API naming, "
                                   "please select one of: {}".format(var_config.prefix, self._cdi_map))

    @property
    def cdi_map(self):
        return self._cdi_map


class ERA5DatasetConfig(CDSDatasetConfig):
    def __init__(self,
                 identifier: str = None,
                 cdi_map: object = None,
                 **kwargs):
        super().__init__(identifier="era5"
                         if identifier is None else identifier,
                         cdi_map=cdi_map,
                         **kwargs)

class AWSDatasetConfig(DatasetConfig):
    # Ref: https://confluence.ecmwf.int/display/CKB/ERA5%3A+data+documentation
    # Map of CMIP6 variable names to ECMWF Grib parameter names
    CMIP6_MAP = {
        "tas":  {"id": 167, "short_name": "2t",           # Near-surface air temperature (CMIP6: tas, ECMWF ID: 167)
                "product_type": "reanalysis", "dataset": "surface-level"},
        "ta":   {"id": 130, "short_name": "t",            # Air temperature at various levels (CMIP6: ta, ECMWF ID: 130)
                "product_type": "reanalysis", "dataset": "pressure-level"},
        "tos":  {"id": 34,  "short_name": "sstk",         # Sea Surface Temperature (CMIP6: tos, ECMWF ID: 34)
                "product_type": "reanalysis", "dataset": "surface-level"},
        "ps":   {"id": 134, "short_name": "sp",           # Surface pressure (CMIP6: ps, ECMWF ID: 134)
                "product_type": "reanalysis", "dataset": "surface-level"},
        # Has to be manually processed from geopotential to geopotential height in post-processing
        "zg":   {"id": 129, "short_name": "z",            # Geopotential height (CMIP6: zg, ECMWF ID: 129)
                "product_type": "reanalysis", "dataset": "pressure-level"},
        "hus":  {"id": 133, "short_name": "q",            # Specific humidity (CMIP6: hus, ECMWF ID: 133)
                "product_type": "reanalysis", "dataset": "pressure-level"},
        # Ref: https://codes.ecmwf.int/grib/param-db/175
        # No reanalysis for this param in AWS ERA5 dataset, only forecast
        # "rlds": {"id": 175, "short_name": "strd"},        # Downward longwave radiation flux at surface (CMIP6: rlds, ECMWF ID: 175)
        #         "product_type": "forecast", "dataset": "accumulation"},
        # Ref: https://codes.ecmwf.int/grib/param-db/169
        # No reanalysis for this param in AWS ERA5 dataset, only forecast
        # "rsds": {"id": 169, "short_name": "ssrd",         # Downward shortwave radiation flux at surface (CMIP6: rsds, ECMWF ID: 169)
        #         "product_type": "forecast", "dataset": "accumulation"},
        "uas":  {"id": 165, "short_name": "10u",          # 10m U-component of wind (CMIP6: uas, ECMWF ID: 165)
            "product_type": "reanalysis", "dataset": "surface-level"},
        "vas":  {"id": 166, "short_name": "10v",          # 10m V-component of wind (CMIP6: vas, ECMWF ID: 166)
            "product_type": "reanalysis", "dataset": "surface-level"},
        "ua":   {"id": 131, "short_name": "u",            # U-component of wind at specific levels (CMIP6: ua, ECMWF ID: 131)
            "product_type": "reanalysis", "dataset": "pressure-level"},
        "va":   {"id": 132, "short_name": "v",            # V-component of wind at specific levels (CMIP6: va, ECMWF ID: 132)
            "product_type": "reanalysis", "dataset": "pressure-level"},
        "sic":  {"id": 31,  "short_name": "ci",           # Sea ice concentration (CMIP6: sic, ECMWF ID: 262001)
            "product_type": "reanalysis", "dataset": "surface-level"},
        "psl":  {"id": 151, "short_name": "msl",          # Sea level pressure (CMIP6: psl, ECMWF ID: 151)
            "product_type": "reanalysis", "dataset": "surface-level"},
    }

    def __init__(self,
                 identifier: str = None,
                 cmip6_map: object = None,
                 **kwargs):
        super().__init__(identifier="aws"
                         if identifier is None else identifier,
                         **kwargs)

        self.cmip6_map = AWSDatasetConfig.CMIP6_MAP
        if cmip6_map is not None:
            self._cmip6_map.update(cmip6_map)

        for var_config in self.variables:
            if var_config.prefix not in self._cmip6_map:
                raise RuntimeError("{} requested but we don't have a map to CDS API naming, "
                                   "please select one of: {}".format(var_config.prefix, self._cmip6_map))

    @property
    def cmip6_map(self):
        return self._cmip6_map

    @cmip6_map.setter
    def cmip6_map(self, value):
        self._cmip6_map = value


class CDSDownloader(ThreadedDownloader):
    def __init__(self,
                 dataset: CDSDatasetConfig,
                 *args,
                 show_progress: bool = False,
                 start_date: object,
                 dataset_name: Union[str, None] = None,
                 product_type: Union[str, None] = None,
                 time: Union[list, None] = None,
                 daily_statistic: str = "daily_mean",
                 time_zone: str = "utc+00:00",
                 derived_frequency: str = "1_hourly",
                 compress: Union[int, None] = None,
                 **kwargs):
        self.client = cds.Client(progress=show_progress)
        self.dataset_name = dataset_name
        self.product_type = product_type
        self.time = time
        # Variables for derived daily statistics
        self.daily_statistic = daily_statistic
        self.time_zone = time_zone
        self.derived_frequency = derived_frequency
        self.compress = compress

        super().__init__(dataset,
                         *args,
                         source_min_frequency=Frequency.YEAR,
                         # TODO: validate handling of hourly data, but it is
                         #  possible as a temporal resolution
                         source_max_frequency=Frequency.HOUR,
                         start_date=start_date,
                         **kwargs)

        self.download_method = self._single_api_download

        if self.max_threads > 10:
            logging.info("Upping connection limit for max_threads > 10")
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=self.max_threads,
                pool_maxsize=self.max_threads
            )
            self.client.session.mount("https://", adapter)

    def _single_api_download(self,
                             var_config: object,
                             req_dates: object,
                             ) -> list:
        """Implements a single download from CDS API

        Args:
            var_config:
            req_dates: The requested dates
        """

        logging.debug("Processing {} dates for {}".format(len(req_dates), var_config))
        monthly_request = self.dataset.frequency < Frequency.DAY

        temp_download_path = os.path.join(var_config.root_path,
                                          self.dataset.location.name,
                                          "temp.{}".format(os.path.basename(
                                              self.dataset.var_filepath(var_config, req_dates))))
        download_path = os.path.join(var_config.root_path,
                                     self.dataset.location.name,
                                     os.path.basename(self.dataset.var_filepath(var_config, req_dates)))
        os.makedirs(os.path.dirname(download_path), exist_ok=True)

        # Default to legacy values if not provided
        if not self.product_type:
            product_type = "reanalysis" if not monthly_request else "monthly_averaged_reanalysis_by_hour_of_day"
        else:
            product_type = self.product_type

        retrieve_dict = {
            "product_type": [product_type,],
            "variable": self.dataset.cdi_map[var_config.prefix],
            "year": [int(req_dates[0].year),],
            "month": list(set(["{:02d}".format(rd.month)
                               for rd in sorted(req_dates)])),
            "format": "netcdf",
            # TODO: explicit, but should be implicit
            "grid": [0.25, 0.25],
            "area": self.dataset.location.bounds,
            "download_format": "unarchived"
        }

        # Add derived dataset-specific keys
        stats_dataset = False
        if self.dataset_name in [
            "derived-era5-pressure-levels-daily-statistics",
            "derived-era5-single-levels-daily-statistics"
        ]:
            stats_dataset = True
            retrieve_dict.update({
                "daily_statistic": self.daily_statistic,
                "time_zone": self.time_zone,
                "frequency": self.derived_frequency
            })

        level_id = "single-levels"
        if var_config.level:
            level_id = "pressure-levels"
            retrieve_dict["pressure_level"] = [var_config.level]

        # Default to legacy values if not provided
        if not self.product_type:
            dataset = "reanalysis-era5-{}{}".format(level_id, "-monthly-means" if monthly_request else "")
        else:
            # TODO: this is a bit of a hack, but it works for now
            # Updating dataset name if multiple pressure levels are requested
            if var_config.level and "single-levels" in self.dataset_name:
                dataset = self.dataset_name.replace("single-levels", "pressure-levels")
            else:
                dataset = self.dataset_name

        # FIXME: this is quite shaky, not using at present
        #    # _, date_end = get_era5_available_date_range(dataset)

        #    # TODO: This updates to dates available for download, prevents
        #    #       redundant downloads but, requires work to prevent
        #    #       postprocess method from running if no downloaded file.
        #    # req_dates = [date for date in req_dates if date <= date_end]
        # END

        if not monthly_request:
            retrieve_dict["day"] = ["{:02d}".format(d) for d in range(1, 32)]

            # No time key required for daily stats dataset, instead uses `time_zone`
            if not stats_dataset:
                if self.time and isinstance(self.time, list):
                    if self.time[0] == "all":
                        time = ["{:02d}:00".format(h) for h in range(0, 24)]
                    else:
                        time = self.time
                else:
                    time = ["12:00",]
                retrieve_dict["time"] = time

        if os.path.exists(temp_download_path):
            raise DownloaderError("{} already exists, this shouldn't be the case, please consider altering the "
                                  "time resolution of request to avoid downloaded data clashes".format(temp_download_path))

        try:
            logging.info("Downloading data for {}...".format(var_config.name))
            logging.debug("Request dataset {} with:\n".format(pformat(retrieve_dict)))
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

        # TODO: there is duplicated / messy code here from CDS API alterations, clean it up
        # New CDSAPI file holds more data_vars than just variable.
        # Omit them when figuring out default CDS variable name.
        omit_vars = {"number", "expver", "time", "date", "valid_time", "latitude", "longitude"}
        data_vars = set(ds.data_vars)
        var_list = list(data_vars.difference(omit_vars))
        if not var_list:
            raise ValueError("No variables found in file")
        elif len(var_list) > 1:
            raise ValueError(f"""Multiple variables found in data file!
                                 There should only be one variable.
                                 {var_list}"""
                            )
        src_var_name = var_list[0]
        var_name = var_config.name

        # Rename time and variable names for consistency
        rename_vars = {
                       src_var_name: var_name,
                       }
        if "date" in ds:
            rename_vars.update({"date": "time"})
        elif "valid_time" in ds:
            rename_vars.update({"valid_time": "time"})

        da = getattr(ds.rename(rename_vars), var_name)

        # This data downloader handles different pressure_levels in independent
        # files rather than storing them all in separate dimension of one array/file.
        if "pressure_level" in da.dims:
            da = da.squeeze(dim="pressure_level").drop_vars("pressure_level")

        if "number" in da.coords:
            da = da.drop_vars("number")

        # Updating coord attribute definitions (needs file read in with `decode_cf=False`)
        if "coordinates" in da.attrs:
            omit_attrs = ["number", "expver", "isobaricInhPa"]
            attributes = re.sub(r"valid_time|date", "time", da.attrs["coordinates"]).split()
            attributes = [attr for attr in attributes if attr not in omit_attrs]
            da.attrs["coordinates"] = " ".join(attributes)

        # Bryn Note:
        # expver = 1: ERA5
        # expver = 5: ERA5T
        # The latest 3 months of data is ERA5T and may be subject to changes.
        # Data prior to this is from ERA5.
        # The new CDSAPI returns combined data when `reanalysis` is requested.
        if 'expver' in ds.coords:
            logging.warning("expver in coordinates, new cdsapi returns ERA5 and "
                            "ERA5T combined, this needs further work: expver needs "
                            "storing for later overwriting")
            # Ref: https://confluence.ecmwf.int/pages/viewpage.action?pageId=173385064
            # da = da.sel(expver=1).combine_first(da.sel(expver=5))
        logging.info("Saving corrected ERA5 file to {}".format(download_path))
        xr_save_netcdf(da, download_path, complevel=self.compress)
        da.close()

        if os.path.exists(temp_download_path):
            logging.debug("Removing {}".format(temp_download_path))
            os.unlink(temp_download_path)

        return [download_path]

    def _single_download(self,
                         var_config: object,
                         req_dates: object) -> list:
        logging.warning("You're not going to get data by calling this! "
                        "Set download_method to an actual implementation.")


class ERA5Downloader(ThreadedDownloader):
    def __init__(self,
                 dataset: ERA5DatasetConfig,
                 *args,
                 show_progress: bool = False,
                 start_date: object,
                 **kwargs):
        warn(f'{self.__class__.__name__} will be deprecated, use CDSDownloader.', DeprecationWarning, stacklevel=2)
        era5_start = dt.date(1940, 1, 1)
        self.client = cds.Client(progress=show_progress)
        logging.getLogger("cdsapi").setLevel(logging.WARNING)

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

        if self.max_threads > 10:
            logging.info("Upping connection limit for max_threads > 10")
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=self.max_threads,
                pool_maxsize=self.max_threads
            )
            self.client.session.mount("https://", adapter)

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
                                          self.dataset.location.name,
                                          "temp.{}".format(os.path.basename(
                                              self.dataset.var_filepath(var_config, req_dates))))
        download_path = os.path.join(var_config.root_path,
                                     self.dataset.location.name,
                                     os.path.basename(self.dataset.var_filepath(var_config, req_dates)))
        os.makedirs(os.path.dirname(download_path), exist_ok=True)

        retrieve_dict = {
            "product_type": [product_type,],
            "variable": self.dataset.cdi_map[var_config.prefix],
            "year": [int(req_dates[0].year),],
            "month": list(set(["{:02d}".format(rd.month)
                               for rd in sorted(req_dates)])),
            # TODO: assumption about the time of day!
            "time": ["12:00",],
            "format": "netcdf",
            # TODO: explicit, but should be implicit
            "grid": [0.25, 0.25],
            "area": self.dataset.location.bounds,
            "download_format": "unarchived"
        }

        level_id = "single-levels"
        if var_config.level:
            level_id = "pressure-levels"
            retrieve_dict["pressure_level"] = [var_config.level]
        dataset = "reanalysis-era5-{}{}".format(level_id, "-monthly-means" if monthly_request else "")

        # FIXME: this is quite shaky, not using at present
        #    # _, date_end = get_era5_available_date_range(dataset)

        #    # TODO: This updates to dates available for download, prevents
        #    #       redundant downloads but, requires work to prevent
        #    #       postprocess method from running if no downloaded file.
        #    # req_dates = [date for date in req_dates if date <= date_end]
        # END

        if not monthly_request:
            retrieve_dict["day"] = ["{:02d}".format(d) for d in range(1, 32)]
            # retrieve_dict["time"] = ["{:02d}:00".format(h) for h in range(0, 24)]

        if os.path.exists(temp_download_path):
            raise DownloaderError("{} already exists, this shouldn't be the case, please consider altering the "
                                  "time resolution of request to avoid downloaded data clashes".format(temp_download_path))

        try:
            logging.info("Downloading data for {}...".format(var_config.name))
            logging.debug("Request dataset {} with:\n".format(pformat(retrieve_dict)))
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

        # TODO: there is duplicated / messy code here from CDS API alterations, clean it up
        # New CDSAPI file holds more data_vars than just variable.
        # Omit them when figuring out default CDS variable name.
        omit_vars = {"number", "expver", "time", "date", "valid_time", "latitude", "longitude"}
        data_vars = set(ds.data_vars)
        var_list = list(data_vars.difference(omit_vars))
        if not var_list:
            raise ValueError(f"No variables found in file")
        elif len(var_list) > 1:
            raise ValueError(f"""Multiple variables found in data file!
                                 There should only be one variable.
                                 {var_list}"""
                            )
        src_var_name = var_list[0]
        var_name = var_config.name

        # Rename time and variable names for consistency
        rename_vars = {
                       src_var_name: var_name,
                       }
        if "date" in ds:
            rename_vars.update({"date": "time"})
        elif "valid_time" in ds:
            rename_vars.update({"valid_time": "time"})

        da = getattr(ds.rename(rename_vars), var_name)

        # This data downloader handles different pressure_levels in independent
        # files rather than storing them all in separate dimension of one array/file.
        if "pressure_level" in da.dims:
            da = da.squeeze(dim="pressure_level").drop_vars("pressure_level")

        if "number" in da.coords:
            da = da.drop_vars("number")

        # Updating coord attribute definitions (needs file read in with `decode_cf=False`)
        if "coordinates" in da.attrs:
            omit_attrs = ["number", "expver", "isobaricInhPa"]
            attributes = re.sub(r"valid_time|date", "time", da.attrs["coordinates"]).split()
            attributes = [attr for attr in attributes if attr not in omit_attrs]
            da.attrs["coordinates"] = " ".join(attributes)

        # Bryn Note:
        # expver = 1: ERA5
        # expver = 5: ERA5T
        # The latest 3 months of data is ERA5T and may be subject to changes.
        # Data prior to this is from ERA5.
        # The new CDSAPI returns combined data when `reanalysis` is requested.
        if 'expver' in ds.coords:
            logging.warning("expver in coordinates, new cdsapi returns ERA5 and "
                            "ERA5T combined, this needs further work: expver needs "
                            "storing for later overwriting")
            # Ref: https://confluence.ecmwf.int/pages/viewpage.action?pageId=173385064
            # da = da.sel(expver=1).combine_first(da.sel(expver=5))
        logging.info("Saving corrected ERA5 file to {}".format(download_path))
        da.to_netcdf(download_path)
        da.close()

        if os.path.exists(temp_download_path):
            logging.debug("Removing {}".format(temp_download_path))
            os.unlink(temp_download_path)

        return [download_path]

    def _single_download(self,
                         var_config: object,
                         req_dates: object) -> list:
        logging.warning("You're not going to get data by calling this! "
                        "Set download_method to an actual implementation.")


def get_era5_available_date_range(dataset: str = "reanalysis-era5-single-levels"):
    """Returns the time range for which ERA5(T) data is available.
    Args:
        dataset: Dataset for which available time range should be returned.
    Returns:
        date_start: Earliest time data is available from.
        date_end: Latest time data available.
    """
    location = f"https://cds.climate.copernicus.eu/api/catalogue/v1/collections/{dataset}"
    res = requests.get(location)

    temporal_interval = res.json()["extent"]["temporal"]["interval"][0]
    time_start, time_end = temporal_interval

    date_start = pd.Timestamp(pd.to_datetime(time_start).date())
    date_end = pd.Timestamp(pd.to_datetime(time_end).date())
    return date_start, date_end

class AWSDownloader(ThreadedDownloader):
    def __init__(self,
                 dataset: CDSDatasetConfig,
                 *args,
                 show_progress: bool = False,
                 start_date: object,
                 end_date: object,
                 compress: Union[int, None] = None,
                 **kwargs):
        # Date ranges available from AWS data
        era5_start = dt.date(1940, 1, 1)
        era5_end = dt.date(2024, 12, 31)
        self.client = cds.Client(progress=show_progress)
        logging.getLogger("cdsapi").setLevel(logging.WARNING)

        if start_date < era5_start:
            raise DownloaderError("{} is before the limited ERA5 date available from AWS of {}".
                                  format(start_date, era5_start))
        elif end_date > era5_end:
            raise DownloaderError("{} is after the limited ERA5 date available from AWS of {}".
                                  format(end_date, era5_end))

        super().__init__(dataset,
                         *args,
                         source_min_frequency=Frequency.YEAR,
                         # TODO: validate handling of hourly data, but it is
                         #  possible as a temporal resolution
                         source_max_frequency=Frequency.HOUR,
                         start_date=start_date,
                         end_date=end_date,
                         **kwargs)

        self.download_method = self._single_api_download
        self.product_type_map = self.__product_type_map()
        self.dataset_map = self.__dataset_map()
        self.compress = compress

    @staticmethod
    def __product_type_map() -> dict:
        """Returns a mapping of variable names to CDS API variables"""
        # Reference following ECMWF Docs on documentation details
        # https://confluence.ecmwf.int/pages/viewpage.action?pageId=85402030#ERA5terminology:analysisandforecast;timeandsteps;instantaneousandaccumulatedandmeanratesandmin/maxparameters-Analysisandforecast
        # Including difference between 'an' and 'fc'
        return {
            "reanalysis": {
                "short-code": "an",
                "help": (
                    "ERA5 Reanalysis. An analysis of the atmospheric conditions is a blend "
                    "of observations with a previous forecast."
                )
            },
            "forecast": {
                "short-code": "fc.sfc",
                "help": (
                    "ERA5 Forecast Data. A forecast starts with an analysis at a specific time "
                    "(the 'initialisation time'), and a model computes the atmospheric conditions "
                    "for a number of 'forecast steps', at increasing 'validity times', into the future."
                )
            },
            "invariant": {
                "short-code": "invariant",
                "help": (
                    "Variables that don't change over time (e.g. land-sea mask, topography, surface type)."
                )
            }
        }

    @staticmethod
    def __dataset_map() -> dict:
        """Returns a mapping of dataset type to CDS short-code"""
        return {
            "pressure-level": {
                "short-code": "pl",
                "product-type": "reanalysis",
                "help": (
                    "Pressure Level data. Variables available on standard pressure levels in the atmosphere "
                    "(e.g. 850 hPa, 500 hPa), such as temperature, geopotential, wind, etc."
                )
            },
            "surface-level": {
                "short-code": "sfc",
                "product-type": "reanalysis",
                "help": (
                    "Surface Level data. Variables at the surface or near-surface, like 2m temperature, "
                    "10m wind, surface pressure, etc."
                )
            },
            "vertically-integrated": {
                "short-code": "vinteg",
                "product-type": "reanalysis",
                "help": (
                    "Vertically Integrated variables. These are quantities integrated through the depth of "
                    "the atmosphere, such as total column water vapor or total column ozone."
                )
            },
            "accumulation": {
                "short-code": "accumu",
                "product-type": "forecast",
                "help": (
                    "Accumulated Forecast Fields. Variables that accumulate over a time interval, "
                    "such as precipitation, snowfall, or runoff."
                )
            },
            "instantaneous": {
                "short-code": "instan",
                "product-type": "forecast",
                "help": (
                    "Instantaneous Forecast Fields. Snapshot values at a specific forecast time, "
                    "e.g. 2m temperature or surface pressure."
                )
            },
            "meanflux": {
                "short-code": "meanflux",
                "product-type": "forecast",
                "help": (
                    "Mean Flux Forecast Fields. Time-averaged fluxes such as sensible heat flux, "
                    "latent heat flux, or radiation components."
                )
            },
            "minmax": {
                "short-code": "minmax",
                "product-type": "forecast",
                "help": (
                    "Minimum/Maximum Forecast Fields. Extremes of a variable over a time period, "
                    "e.g. daily maximum temperature or minimum relative humidity."
                )
            },
            "invariant": {
                "short-code": "invariant",
                "product-type": "invariant",
                "help": (
                    "Invariant Fields. Static variables that do not change over time, "
                    "such as land-sea mask, topography, or surface type."
                )
            }
        }

    @staticmethod
    @lru_cache
    def __list_matching_files(prefix, start_date, end_date, cmip6_variable, ecmwf_variable, bucket_name, multiple_levels: bool):
        s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED))
        bucket = s3.Bucket(bucket_name)
        matching_files = defaultdict(list)

        current = start_date.replace(day=1)
        while current <= end_date:
            year_month = current.strftime("%Y%m")
            full_prefix = f"{prefix}{year_month}/"
            for obj in bucket.objects.filter(Prefix=full_prefix):
                nc_file_path = obj.key
                # TODO: The filename seems to follow this pattern:
                # {dataset}.{grib_table}_{parameter_id}_{short_name}.{grid_config}.{start_datetime}_{end_datetime}.nc
                if not nc_file_path.endswith(".nc"):
                    continue
                # Filter by date range
                timestamp_part = nc_file_path.split('.')[-2]
                file_start_date, file_end_date = timestamp_part.split('_')
                try:
                    file_start_date = dt.datetime.strptime(file_start_date, "%Y%m%d%H")
                    file_end_date = dt.datetime.strptime(file_end_date, "%Y%m%d%H")
                except ValueError:
                    continue

                if multiple_levels:
                    # Stores variables with multiple levels in separate daily files
                    # e5.oper.an.pl
                    if not (start_date <= file_start_date <= end_date):
                        continue
                else:
                    # Stores surface variables in separate monthly files
                    # e5.oper.an.sfc
                    if not (start_date <= end_date):
                        continue
                # Filter by parameter
                pattern = rf"\.(\d+_\d+_{ecmwf_variable})\." # Get the parameter details section of filename
                match = re.search(pattern, nc_file_path)
                if not match:
                    continue
                grib_table, parameter_id, ecmwf_short_name = match.group(1).split("_")
                matching_files[cmip6_variable].append(f"s3://{bucket_name}/" + nc_file_path)
            # Move to the next month (even if no. of days less than 31 days)
            current += dt.timedelta(days=32)
            current = current.replace(day=1)

        return matching_files

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
        var_config_collection = defaultdict(list)

        # Collate variables with different pressure levels together
        # Avoids needing to download the same file repeatedly from AWS.
        for var_config in self.dataset.variables:
            dates = self.dataset.filter_extant_data(var_config, self.dates)

            for req_date_batch in batch_requested_dates(dates=dates, attribute=self.request_frequency.attribute):
                logging.info("Processing single download for {} with {} dates".
                             format(var_config.name, len(req_date_batch)))

                var_config_collection[var_config.prefix].append((var_config, req_date_batch))

        for var_collection in var_config_collection.values():
            req_list.append([var_collection])

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

    def _single_api_download(self,
                            args: list,
                             ) -> list:
        """Implements a single download from CDS API

        Args:
            args: A list of tuples containing (`var_config`, `req_dates`)
        """
        # logging.debug("Processing {} dates for {}".format(len(req_dates), var_config))
        # TODO: Add monthly request handling
        #       for AWS data, this is not currently supported
        #       as the data is not available in monthly files
        monthly_request = self.dataset.frequency < Frequency.DAY
        if monthly_request:
            raise DownloaderError("Monthly requests are not supported for AWS data, use `download_cds` instead")

        bucket_name = "nsf-ncar-era5"
        product_type_map = self.__product_type_map()
        dataset_map = self.__dataset_map()

        # Extract root_path from dataset config
        temp_download_path = os.path.join(self.dataset._root_path, "cache")
        fs = fsspec.filesystem("filecache", target_protocol="s3", target_options={"anon": True},
                                cache_storage=temp_download_path)

        # Loop through different pressure levels
        downloaded_paths = []
        for var_levels in args:
            var_config, req_dates = var_levels
            start_date = req_dates[0]
            end_date = req_dates[-1]

            start_dt = dt.datetime.combine(start_date, dt.time(0, 0))
            end_dt = dt.datetime.combine(end_date, dt.time(0, 0))

            # Retrieve filtered file list
            cmip6_variable_code = var_config.prefix
            ecmwf_variable_code = self.dataset.cmip6_map[cmip6_variable_code]["short_name"]

            product = self.dataset.cmip6_map[cmip6_variable_code]["product_type"]
            dataset = self.dataset.cmip6_map[cmip6_variable_code]["dataset"]

            product_code = product_type_map[product]["short-code"]
            dataset_code = dataset_map[dataset]["short-code"]

            logging.info(f"Selected ERA5 product type: {product_code}")
            logging.info(f"Selected ERA5 dataset type: {dataset_code}")

            # Parse prefix
            if product_code == "invariant":
                prefix = f"e5.oper.{product_code}/"
            else:
                prefix = f"e5.oper.{product_code}.{dataset_code}/"

            level = var_config.level
            if level and dataset != "pressure-level":
                raise ValueError(f"Level `{level}` is not supported for `{dataset}` dataset type, "
                                    "this is a surface or near-surface variable"
                )

            filtered_files = self.__list_matching_files(prefix, start_dt, end_dt,
                                cmip6_variable_code, ecmwf_variable_code, bucket_name,
                                multiple_levels=True if level else False)
            logging.debug(f"Files to download:\n\t{'\n\t'.join(filtered_files[cmip6_variable_code])}")

            download_path = os.path.join(var_config.root_path,
                                        self.dataset.location.name,
                                        os.path.basename(self.dataset.var_filepath(var_config, req_dates)))

            os.makedirs(os.path.dirname(temp_download_path), exist_ok=True)
            os.makedirs(os.path.dirname(download_path), exist_ok=True)

            try:
                logging.info(f"Downloading data for {var_config.name}...")
                logging.debug(f"Request file:\n{filtered_files[cmip6_variable_code]}")

                ds = xr.open_mfdataset(
                    [fs.open(filtered_file, mode="rb") for filtered_file in filtered_files[cmip6_variable_code]],
                    combine="by_coords",
                    engine="h5netcdf",
                    parallel=True,
                    chunks={},
                    )
            except Exception as e:
                logging.exception("{} not downloaded, look at the problem".format(temp_download_path))
                self.missing_dates.extend(req_dates)
                return []

            # Extract pressure level
            if "level" in ds.dims:
                # Clearly have multiple pressure levels
                ds = ds.sel(level=level).drop_vars("level")
            else:
                # Surface level data
                ds = ds.sel(time=slice(start_dt, end_dt + dt.timedelta(hours=23)))

            # Roll the data to have the 0 degree longitude at the center
            ds.coords["longitude"] = (ds.coords["longitude"] + 180) % 360 - 180
            ds = ds.sortby(ds.longitude)

            # Extract region
            max_lat, min_lon, min_lat, max_lon = self.dataset.location.bounds
            ds_region = ds.sel(longitude=(ds.longitude <= max_lon) | (ds.longitude >= min_lon),
                            latitude=(ds.latitude <= max_lat) & (ds.latitude >= min_lat))

            # Figure out the data variable name.
            # It should have the following three dimensions by this point:
            expected_dims = ["time", "latitude", "longitude"]
            for var in ds_region.data_vars:
                var_dims = ds_region[var].dims
                if all([dim in var_dims for dim in expected_dims]):
                    src_var_name = var

            var_name = var_config.name

            # Rename variable name for consistency
            rename_vars = {src_var_name: var_name}
            da = getattr(ds_region.rename(rename_vars), var_name)

            logging.info("Saving corrected ERA5 file to {}".format(download_path))
            xr_save_netcdf(da, download_path, complevel=self.compress)
            ds.close()

            downloaded_paths.append(download_path)

        return downloaded_paths

    def _single_download(self,
                         var_config: object,
                         req_dates: object) -> list:
        logging.warning("You're not going to get data by calling this! "
                        "Set download_method to an actual implementation.")


def cds_main():
    args = CDSDownloadArgParser().add_var_specs().add_cds_specs().add_derived_specs().add_workers().parse_args()

    logging.info("CDS Data Downloading")

    location = Location(
        name=args.hemisphere,
        north=args.hemisphere == "north",
        south=args.hemisphere == "south",
    )

    dataset = CDSDatasetConfig(
        levels=args.levels,
        location=location,
        var_names=args.vars,
        frequency=getattr(Frequency, args.frequency),
        output_group_by=getattr(Frequency, args.output_group_by),
        config_path=args.config,
        overwrite=args.overwrite_config,
    )

    for start_date, end_date in zip(args.start_dates, args.end_dates):
        logging.info("Downloading between {} and {}".format(start_date, end_date))
        cds = CDSDownloader(
            dataset,
            start_date=start_date,
            end_date=end_date,
            max_threads=args.workers,
            request_frequency=getattr(Frequency, args.output_group_by),
            dataset_name=args.dataset,
            product_type=args.product_type,
            time=args.time,
            daily_statistic=args.daily_statistic,
            time_zone=args.time_zone,
            derived_frequency=args.derived_frequency,
            compress=args.compress,
        )
        cds.download()

        dataset.save_data_for_config(
            source_files=cds.files_downloaded,
            var_filter_list=["lambert_azimuthal_equal_area"],
        )


def era5_main():
    args = DownloadArgParser().add_var_specs().add_workers().parse_args()

    logging.info("ERA5 Data Downloading")

    location = Location(
        name=args.hemisphere,
        north=args.hemisphere == "north",
        south=args.hemisphere == "south",
    )

    dataset = ERA5DatasetConfig(
        levels=args.levels,
        location=location,
        var_names=args.vars,
        frequency=getattr(Frequency, args.frequency),
        output_group_by=getattr(Frequency, args.output_group_by),
        config_path=args.config,
        overwrite=args.overwrite_config,
    )

    for start_date, end_date in zip(args.start_dates, args.end_dates):
        logging.info("Downloading between {} and {}".format(start_date, end_date))
        era5 = ERA5Downloader(
            dataset,
            start_date=start_date,
            end_date=end_date,
            max_threads=args.workers,
            request_frequency=getattr(Frequency, args.output_group_by)
        )
        era5.download()

        dataset.save_data_for_config(
            source_files=era5.files_downloaded,
            var_filter_list=["lambert_azimuthal_equal_area"],
        )

def aws_main():
    args = AWSDownloadArgParser().add_var_specs().add_aws_specs().add_workers().parse_args()

    logging.info("AWS Data Downloading")

    location = Location(
        name=args.hemisphere,
        north=args.hemisphere == "north",
        south=args.hemisphere == "south",
    )

    dataset = AWSDatasetConfig(
        levels=args.levels,
        location=location,
        var_names=args.vars,
        frequency=getattr(Frequency, args.frequency),
        output_group_by=getattr(Frequency, args.output_group_by),
        config_path=args.config,
        overwrite=args.overwrite_config,
    )

    for start_date, end_date in zip(args.start_dates, args.end_dates):
        logging.info("Downloading between {} and {}".format(start_date, end_date))
        aws = AWSDownloader(
            dataset,
            start_date=start_date,
            end_date=end_date,
            compress=args.compress,
            max_threads=args.workers,
            request_frequency=getattr(Frequency, args.output_group_by),
        )
        aws.download()

        dataset.save_data_for_config(
            source_files=aws.files_downloaded,
            var_filter_list=["lambert_azimuthal_equal_area"],
        )

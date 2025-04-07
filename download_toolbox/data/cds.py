import datetime as dt
import logging
import re
import requests
import requests.adapters
import os

import cdsapi as cds
import pandas as pd
import xarray as xr

from pprint import pformat
from typing import Union
from warnings import warn

from download_toolbox.dataset import DatasetConfig
from download_toolbox.data.utils import batch_requested_dates
from download_toolbox.cli import DownloadArgParser
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
                 **kwargs):
        self.client = cds.Client(progress=show_progress)
        self.dataset_name = dataset_name
        self.product_type = product_type
        self.time = time
        # Variables for derived daily statistics
        self.daily_statistic = daily_statistic
        self.time_zone = time_zone
        self.derived_frequency = derived_frequency

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
        if self.dataset_name in [
            "derived-era5-pressure-levels-daily-statistics",
            "derived-era5-single-levels-daily-statistics"
        ]:
            retrieve_dict.pop("time")
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


def cds_main():
    args = DownloadArgParser().add_var_specs().add_cds_specs().add_derived_specs().add_workers().parse_args()

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
            derived_frequency=args.derived_frequency
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

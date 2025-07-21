import datetime as dt
import logging
import re
import requests
import requests.adapters
import os
import zipfile

import cdsapi as cds
import pandas as pd
import xarray as xr

from pprint import pformat
from typing import Union

from download_toolbox.cli import CDSDownloadArgParser, DownloadArgParser
from download_toolbox.dataset import DatasetConfig
from download_toolbox.data.utils import xr_save_netcdf
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
                 # TODO: short_names is experimental for CDS downloads only, but maybe should be moved to DatasetConfig
                 long_names: list = None,
                 **kwargs):
        super().__init__(identifier="cds"
                         if identifier is None else identifier,
                         **kwargs)

        self._cdi_map = CDSDatasetConfig.CDI_MAP
        if cdi_map is not None:
            self._cdi_map.update(cdi_map)

        if long_names is not None:
            self._cdi_map = dict(zip([var_config.prefix for var_config in self.variables], long_names))

        for var_config in self.variables:
            if var_config.prefix not in self._cdi_map:
                raise RuntimeError("{} requested but we don't have a map to CDS API naming, "
                                   "please select one of: {}".format(var_config.prefix, self._cdi_map))

    @property
    def cdi_map(self):
        return self._cdi_map


class CDSDownloader(ThreadedDownloader):
    def __init__(self,
                 dataset: CDSDatasetConfig,
                 *args,
                 show_progress: bool = False,
                 start_date: object,
                 dataset_name: Union[str, None] = None,
                 product_type: Union[list, None] = None,
                 time: Union[list, None] = None,
                 daily_statistic: str = "daily_mean",
                 time_zone: str = "utc+00:00",
                 derived_frequency: str = "1_hourly",
                 compress: Union[int, None] = None,
                 request_args: [dict, None] = None,
                 zipped: bool = False,
                 **kwargs):
        self._client = cds.Client(progress=show_progress)
        self._dataset_name = dataset_name
        self._product_type = product_type
        self._time = time
        # Variables for derived daily statistics
        self._daily_statistic = daily_statistic
        self._time_zone = time_zone
        self._derived_frequency = derived_frequency
        self._compress = compress
        self._request_args = request_args
        self._zipped = zipped

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
            self._client.session.mount("https://", adapter)

    def _single_api_download(self,
                             var_config: object,
                             req_dates: object) -> list:
        """Implements a single download from CDS API

        Args:
            var_config:
            req_dates: The requested dates
        """

        logging.debug("Processing {} dates for {}".format(len(req_dates), var_config))
        monthly_request = self.dataset.frequency < Frequency.DAY

        retrieve_ext = "zip" if self._zipped else "nc"
        temp_download_path = os.path.join(var_config.root_path,
                                          self.dataset.location.name,
                                          "temp.{}".format(os.path.basename(
                                              self.dataset.var_filepath(var_config, req_dates,
                                                                        file_extension=retrieve_ext))))
        download_path = os.path.join(var_config.root_path,
                                     self.dataset.location.name,
                                     os.path.basename(self.dataset.var_filepath(var_config, req_dates)))
        os.makedirs(os.path.dirname(download_path), exist_ok=True)

        if os.path.exists(download_path):
            logging.warning(f"We have a downloaded file available, skipping: {download_path}")
            return [download_path]

        # Default to legacy values if not provided
        if not self._product_type:
            product_type = ["reanalysis",] if not monthly_request else ["monthly_averaged_reanalysis_by_hour_of_day",]
        else:
            product_type = self._product_type

        retrieve_dict = {
            "product_type": product_type,
            "variable": self.dataset.cdi_map[var_config.prefix],
            "year": [int(req_dates[0].year),],
            "month": list(set(["{:02d}".format(rd.month)
                               for rd in sorted(req_dates)])),
        }

        if not self._zipped:
            retrieve_dict.update({
                "format": "netcdf",
                "grid": [0.25, 0.25],
                "area": self.dataset.location.bounds,
                "download_format": "unarchived"
            })

        # Add derived dataset-specific keys
        stats_dataset = False
        if self._dataset_name in [
            "derived-era5-pressure-levels-daily-statistics",
            "derived-era5-single-levels-daily-statistics"
        ]:
            stats_dataset = True
            retrieve_dict.update({
                "daily_statistic": self._daily_statistic,
                "time_zone": self._time_zone,
                "frequency": self._derived_frequency
            })

        level_id = "single-levels"
        if var_config.level:
            level_id = "pressure-levels"
            retrieve_dict["pressure_level"] = [var_config.level]

        # Default to legacy values if not provided
        if not self._product_type:
            dataset = "reanalysis-era5-{}{}".format(level_id, "-monthly-means" if monthly_request else "")
        else:
            # TODO: this is a bit of a hack, but it works for now
            # Updating dataset name if multiple pressure levels are requested
            if var_config.level and "single-levels" in self._dataset_name:
                dataset = self._dataset_name.replace("single-levels", "pressure-levels")
            else:
                dataset = self._dataset_name

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
                if self._time and isinstance(self._time, list):
                    if self._time[0] == "all":
                        time = ["{:02d}:00".format(h) for h in range(0, 24)]
                    else:
                        time = self._time
                else:
                    time = ["12:00",]
                retrieve_dict["time"] = time

        if self._request_args is not None:
            logging.debug("Updating request with dictionary {}".format(self._request_args))
            retrieve_dict.update(self._request_args)

        if os.path.exists(temp_download_path):
            # TODO: we need a better mechanism for keeping temp files and reprocessing
            if self._zipped:
                logging.warning("{} already exists, all we can do is assume to try and reprocess".format(temp_download_path))
            else:
                raise DownloaderError("{} already exists, this shouldn't be the case".format(temp_download_path))
        else:
            try:
                logging.info("Downloading data for {}...".format(var_config.name))
                logging.debug("Request dataset {} with:\n".format(pformat(retrieve_dict)))
                self._client.retrieve(
                    dataset,
                    retrieve_dict,
                    temp_download_path)
                logging.info("Download completed: {}".format(temp_download_path))
            # cdsapi uses raise Exception in many places, so having a catch-all is appropriate
            except Exception as e:
                logging.exception("{} not downloaded, look at the problem".format(temp_download_path))
                self.missing_dates.extend(req_dates)
                return []

        if self._zipped:
            zf = zipfile.ZipFile(temp_download_path)
            zip_output_path = os.path.join(var_config.root_path,
                                           self.dataset.location.name)
            zipped_data_files = [df_name for df_name in zf.namelist()
                                 if df_name.endswith(".nc")
                                 and not os.path.exists(os.path.join(zip_output_path, df_name))]
            zf.extractall(path=zip_output_path,
                          members=zipped_data_files)

            # For the moment we'll keep the zips
            temp_download_path = [os.path.join(zip_output_path, zf) for zf in zipped_data_files]
            ds = xr.open_mfdataset(temp_download_path)
        else:
            ds = xr.open_dataset(temp_download_path)

        # TODO: there is duplicated / messy code here from CDS API alterations, clean it up
        # New CDSAPI file holds more data_vars than just variable.
        # Omit them when figuring out default CDS variable name.
        omit_vars = {"number", "expver", "time", "date", "valid_time", "latitude", "longitude", "time_counter_bnds"}
        data_vars = set(ds.data_vars)
        var_list = list(data_vars.difference(omit_vars))
        if not var_list:
            raise ValueError("No variables found in file")
        elif len(var_list) > 1:
            raise ValueError(f"""Multiple variables found in data file!
                                 There should only be one variable.
                                 {var_list}""")
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
        elif "time_counter" in ds:
            rename_vars.update({"time_counter": "time"})

        if all([f in ds for f in ["nav_lat", "nav_lon"]]):
            logging.warning("We have nav_lat and nav_lon which suggests a tripolar grid"
                            " which we cannot convert within the downloader yet")
            northing, westing, southing, easting = self.dataset.location.bounds
            # FIXME: naively grab the output based on the latitude
            ds = ds.where(((ds.nav_lat <= northing) & (ds.nav_lat >= southing)).compute(), drop=True)

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
        if 'expver' in da.coords:
            logging.warning("expver in coordinates, new cdsapi returns ERA5 and "
                            "ERA5T combined, this needs further work: expver needs "
                            "storing for later overwriting")
            # Ref: https://confluence.ecmwf.int/pages/viewpage.action?pageId=173385064
            # da = da.sel(expver=1).combine_first(da.sel(expver=5))
            da = da.drop_vars("expver")
        logging.info("Saving corrected CDS file to {}".format(download_path))
        xr_save_netcdf(da, download_path, complevel=self._compress)
        da.close()

        if type(temp_download_path) is not list:
            temp_download_path = [temp_download_path,]
        for tdp in temp_download_path:
            if os.path.exists(tdp):
                logging.info("Removing {}".format(tdp))
                os.unlink(tdp)

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
    args, request_args = (CDSDownloadArgParser().
                          add_var_specs().
                          add_cds_specs().
                          add_derived_specs().
                          add_workers().
                          add_extra_args([
                            (("-z", "--zipped"), dict(action="store_true", default=False,
                                                      help="Zipped version only available, changes request "
                                                           "setup and post-processing prior to save"))
                          ]).
                          parse_known_args())

    logging.info("CDS Data Downloading")

    location = Location(
        name=args.hemisphere,
        north=args.hemisphere == "north",
        south=args.hemisphere == "south",
    )

    dataset = CDSDatasetConfig(
        identifier=args.identifier,
        levels=args.levels,
        location=location,
        var_names=args.vars,
        frequency=getattr(Frequency, args.frequency),
        output_group_by=getattr(Frequency, args.output_group_by),
        config_path=args.config,
        overwrite=args.overwrite_config,
        long_names=args.long_names
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
            request_args=request_args,
            zipped=args.zipped
        )
        cds.download()

        dataset.save_data_for_config(
            source_files=cds.files_downloaded,
            var_filter_list=["lambert_azimuthal_equal_area"],
        )


def era5_main():
    args = DownloadArgParser().add_var_specs().add_workers().parse_args()
    # This is a removed version, there are  add_cds_specs().add_derived_specs()
    logging.warning("\n\n{}\n\nERA5 Data Downloading is now performed via download_cds, "
                    "please use that instead as this endpoint will be removed\n\n{}\n\n".
                    format("="*80, "="*80))

    location = Location(
        name=args.hemisphere,
        north=args.hemisphere == "north",
        south=args.hemisphere == "south",
    )
    request_frequency = getattr(Frequency, args.frequency)
    nonlevel_vars = [(args.vars[i], vals) for i, vals in enumerate(args.levels) if vals is None]
    level_vars = [(args.vars[i], vals) for i, vals in enumerate(args.levels) if vals is not None]

    for configuration in [nonlevel_vars, level_vars]:
        var_names, levels = zip(*configuration)

        level_id = "single-levels"
        if levels[0] is not None:
            level_id = "pressure-levels"

        monthly_request = request_frequency < Frequency.DAY
        dataset_name = "reanalysis-era5-{}{}".format(level_id, "-monthly-means" if monthly_request else "")
        product_type = "reanalysis" \
            if not monthly_request \
            else "monthly_averaged_reanalysis_by_hour_of_day"

        dataset = CDSDatasetConfig(
            identifier="era5" if args.identifier is None else args.identifier,
            levels=levels,
            location=location,
            var_names=var_names,
            frequency=request_frequency,
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
                dataset_name=dataset_name,
                product_type=product_type,
            )
            cds.download()

            dataset.save_data_for_config(
                source_files=cds.files_downloaded,
                var_filter_list=["lambert_azimuthal_equal_area"],
            )

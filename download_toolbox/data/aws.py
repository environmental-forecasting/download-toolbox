import concurrent
import datetime as dt
import logging
import os
import re
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from functools import lru_cache

import boto3
import xarray as xr
from botocore import UNSIGNED
from botocore.config import Config
from functools import partial

from download_toolbox.cli import AWSDownloadArgParser
from download_toolbox.data.cds import CDSDatasetConfig
from download_toolbox.data.utils import batch_requested_dates, s3_file_download, xr_save_netcdf
from download_toolbox.dataset import DatasetConfig
from download_toolbox.download import DownloaderError, ThreadedDownloader
from download_toolbox.location import Location
from download_toolbox.time import Frequency


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


class AWSDownloader(ThreadedDownloader):
    """
    Downloader class for retrieving ERA5 climate reanalysis data from AWS.

    Inherits from `ThreadedDownloader`, allowing concurrent downloads of variables
    and date configurations.
    """
    def __init__(self,
                 dataset: CDSDatasetConfig,
                 *args,
                 start_date: object,
                 end_date: object,
                 delete_cache: bool = False,
                 cache_only: bool = False,
                 compress: int = 0,
                 **kwargs):
        """
        Initialise the AWSDownloader instance.

        Args:
            dataset: Dataset configuration object.
            *args: Additional positional arguments to pass to the base class.
            start_date: Start date for the data to download.
            end_date: End date for the data to download.
            delete_cache: If `True`, delete the cache after download.
            cache_only: If `True`, only download files to cache and return.
            compress: Compression level for saved NetCDF files.
                      0 or `None` for no compression.
                      (Defaults to 0. ).
            **kwargs: Additional keyword arguments passed to the base class.

        Raises:
            DownloaderError: If `start_date` or `end_date` are outside AWS ERA5 data range.
        """
        # Date ranges available from AWS data
        era5_start = dt.date(1940, 1, 1)
        era5_end = dt.date(2024, 12, 31)
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
        self.delete_cache = delete_cache
        self.cache_only = cache_only
        self.compress = compress

    @staticmethod
    def __product_type_map() -> dict:
        """
        Get mapping of short codes for product types.

        Returns:
            Product type mapping with keys like "reanalysis", "forecast", etc.

        Notes:
            * Reference following ECMWF Docs on documentation details
            * https://confluence.ecmwf.int/pages/viewpage.action?pageId=85402030#ERA5terminology:analysisandforecast;timeandsteps;instantaneousandaccumulatedandmeanratesandmin/maxparameters-Analysisandforecast
            * Including difference between 'an' and 'fc'
        """
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
        """
        Get mapping of dataset type long name to ECMWF short-code and product types.

        Returns:
            Dataset type mapping for ERA5 datasets.
        """
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
    def __list_matching_files(prefix: str,
                              start_date: dt.datetime,
                              end_date: dt.datetime,
                              cmip6_variable: str,
                              ecmwf_variable: str,
                              bucket_name: str,
                              multiple_levels: bool,
                              ) -> dict:
        """
        AWS S3 file paths matching date range and variable filters.

        Args:
            prefix: S3 prefix path for files.
            start_date: Start datetime to filter.
            end_date: End datetime to filter.
            cmip6_variable: Variable name in CMIP6 format.
            ecmwf_variable: Variable name in ECMWF format.
            bucket_name: AWS S3 bucket name.
            multiple_levels: Whether variable is multi-level (pressure level) or not.

        Returns:
            Dictionary of matching files grouped by variable.
        """
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
                matching_files[cmip6_variable].append(nc_file_path)
            # Move to the next month (even if no. of days less than 31 days)
            current += dt.timedelta(days=32)
            current = current.replace(day=1)

        return matching_files

    def download(self):
        """
        Perform multi-threaded download of ERA5 data from AWS.

        Collects variable and date configurations, batches them for concurrent downloading,
        and uses `ProcessPoolExecutor` to perform downloads in parallel.

        Returns nothing, relies on _single_download to implement
        appropriate updates to this object to record state changes arising from
        downloading.

        Updates the internal `_files_downloaded` list with paths to downloaded files.
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

            with ProcessPoolExecutor(max_workers=max_workers) as executor:
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

    @staticmethod
    def _preprocess(ds: xr.Dataset,
                    start_dt: dt.datetime,
                    end_dt: dt.datetime,
                    level: int,
                    bounds: list[int],
                    ) -> xr.Dataset:
        """
        Preprocess individual xarray datasets before combining.

        This method performs the following preprocessing steps:
        - Selects the appropriate pressure level (if available).
        - Filters data based on the given time range.
        - Rolls the longitude coordinate to center the 0° longitude.
        - Extracts a specific geographical region based on the provided latitude and longitude bounds.

        Args:
            ds: Dataset to be processed.
            start_dt: The start datetime for the time range to filter.
            end_dt: The end datetime for the time range to filter.
            level: The pressure level to select if multiple levels are available.
            bounds: A list containing the bounds for the region to extract,
                    in the order [max_lat, min_lon, min_lat, max_lon].

        Returns:
            Processed Dataset with specified region and time range.
        """
        # Extract pressure level
        if "level" in ds.dims:
            # Clearly have multiple pressure levels
            ds = ds.sel(level=level).drop_vars("level")
        else:
            # Surface level data
            ds = ds.sel(time=slice(start_dt, end_dt + dt.timedelta(hours=23)))

        # Extract region
        max_lat, min_lon, min_lat, max_lon = bounds
        lon_mask = (ds.longitude <= max_lon) | (ds.longitude >= min_lon)
        lat_mask = (ds.latitude <= max_lat) & (ds.latitude >= min_lat)
        ds_region = ds.sel(longitude=lon_mask, latitude=lat_mask)

        return ds_region

    def _single_api_download(self,
                            args: list,
                            ) -> list:
        """
        Perform a single-process download batch from AWS based on variable and date ranges.

        Args:
            args: A list of tuples of (var_config, req_dates) for download.

        Returns:
            List of paths to successfully downloaded files.

        Raises:
            DownloaderError: If an unsupported frequency is encountered.
            ValueError: If pressure levels are used with an unsupported dataset.
        """
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

        # Loop through different pressure levels
        downloaded_paths = []
        downloaded_files = []
        for var_levels in args:
            var_config, req_dates = var_levels
            logging.debug("Processing {} dates for {}".format(len(req_dates), var_config))
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

            cached_files = []
            for filtered_file in filtered_files[cmip6_variable_code]:
                cached_file = os.path.join(temp_download_path, os.path.basename(filtered_file))
                s3_file_download(bucket_name, filtered_file, cached_file)
                cached_files.append(cached_file)
                downloaded_files.append(cached_file)

            dataset_preprocess = partial(self._preprocess,
                                               start_dt=start_dt,
                                               end_dt=end_dt,
                                               level=level,
                                               bounds=self.dataset.location.bounds,
                                               )

            try:
                logging.info(f"Downloading data for {var_config.name}...")
                logging.debug(f"Request file:\n{filtered_files[cmip6_variable_code]}")

                ds = xr.open_mfdataset(
                    cached_files,
                    data_vars="minimal",
                    coords="minimal",
                    combine="by_coords",
                    engine="h5netcdf",
                    preprocess=dataset_preprocess,
                    parallel=True,
                    chunks={"time": 24},
                    )

                if self.cache_only:
                    continue
            except Exception as e:
                logging.exception("{} not downloaded, look at the problem".format(temp_download_path))
                self.missing_dates.extend(req_dates)
                continue

            # Roll the data to have the 0 degree longitude at the center
            ds.coords["longitude"] = (ds.coords["longitude"] + 180) % 360 - 180
            ds = ds.sortby(ds.longitude)

            # Figure out the data variable name.
            # It should have the following three dimensions by this point:
            expected_dims = ["time", "latitude", "longitude"]
            for var in ds.data_vars:
                var_dims = ds[var].dims
                if all([dim in var_dims for dim in expected_dims]):
                    src_var_name = var
                    break

            var_name = var_config.name
            rename_vars = {src_var_name: var_name}
            da = getattr(ds.rename(rename_vars), var_name)

            logging.info("Saving corrected ERA5 file to {}".format(download_path))
            xr_save_netcdf(da, download_path, complevel=self.compress)
            ds.close()

            downloaded_paths.append(download_path)

        # Delete cached files if requested
        if self.delete_cache:
            for cached_file in downloaded_files:
                if os.path.exists(cached_file):
                    os.remove(cached_file)

        return downloaded_paths

    def _single_download(self,
                         args: list) -> list:
        logging.warning("You're not going to get data by calling this! "
                        "Set download_method to an actual implementation.")

def main():
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
            delete_cache=args.delete_cache,
            cache_only=args.cache_only,
            compress=args.compress,
            max_threads=args.workers,
            request_frequency=getattr(Frequency, args.output_group_by),
        )
        aws.download()

        dataset.save_data_for_config(
            source_files=aws.files_downloaded,
            var_filter_list=["lambert_azimuthal_equal_area"],
        )

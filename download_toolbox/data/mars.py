import logging
import os

from typing import Union

import ecmwfapi
import xarray as xr

from download_toolbox.cli import DownloadArgParser, csv_arg
from download_toolbox.dataset import DatasetConfig
from download_toolbox.data.utils import xr_save_netcdf
from download_toolbox.download import Downloader, ThreadedDownloader, DownloaderError
from download_toolbox.location import Location
from download_toolbox.time import Frequency


class MARSDownloadArgParser(DownloadArgParser):
    def __init__(self,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.add_argument("-i", "--identifier",
                          help="Name of the output dataset where it's stored, overriding default",
                          default="mars",
                          type=str)
        self.add_argument("--compress",
                          help="Provide an integer from 1-9 (low to high) on how much to compress the output netCDF",
                          default=None,
                          type=int)

    def add_var_specs(self):
        super().add_var_specs()
        self.add_argument("-s", "--system",
                          help="ECMWF System identifier",
                          type=int,
                          default=5)
        self.add_argument("product_type",
                          help="ECMWF product type",
                          type=str,
                          default=None)
        self.add_argument("stream",
                          help="ECMWF product stream",
                          type=str,
                          default=None)
        self.add_argument("params",
                          help="If provided, this will override name mappings for the given variable prefixes",
                          default=None,
                          type=csv_arg)
        self.add_argument("attributes",
                          help="If provided, this will override name mappings for the given variable prefixes",
                          default=None,
                          type=csv_arg)
        return self


class MARSDataset(DatasetConfig):
    def __init__(self,
                 *args,
                 identifier: str = None,
                 params: list = None,
                 attributes: list = None,
                 **kwargs):
        super().__init__(*args,
                         identifier="mars"
                         if identifier is None else identifier,
                         **kwargs)

        # Handle reverse mapping of keyword parameters back to arguments: it is a hack
        # but allows the reconstruction of the object
        if "mars_mapping" in kwargs and params is None and attributes is None:
            params = [v["param"] for v in kwargs["mars_mapping"].values()]
            attributes = [v["attribute"] for v in kwargs["mars_mapping"].values()]

        if not(len(params) == len(attributes) == len(self.var_prefixes)):
            raise AttributeError("The number of parameter strings, attributes and variables must match: {} vs {} vs {}"
                                 .format(len(params), len(attributes), len(self.variables)))

        self._mars_mapping = {var_prefix: dict(param=params[order], attribute=attributes[order])
                              for order, var_prefix in enumerate(self.var_prefixes)}

    @property
    def mars_mapping(self):
        return self._mars_mapping


class MARSDownloader(Downloader):
    def __init__(self,
                 dataset: MARSDataset,
                 product_type: str,
                 stream: str,
                 *args,
                 product_class: str = "od",
                 system: int = 5,
                 compress: Union[int, None] = None,
                 request_args: [dict, None] = None,
                 **kwargs):
        self._product_class = product_class
        self._product_type = product_type
        self._stream = stream
        self._system = system
        self._compress = compress
        self._request_args = request_args

        self._server = ecmwfapi.ECMWFService("mars")

        super().__init__(dataset,
                         *args,
                         # TODO: not true, this could be max HOUR, need to implement
                         source_min_frequency=Frequency.YEAR,
                         source_max_frequency=Frequency.MONTH,
                         **kwargs)

    def _single_download(self,
                         var_config: object,
                         req_dates: object) -> list:
        logging.debug("Processing {} dates for {}".format(len(req_dates), var_config))
        temp_download_path = os.path.join(var_config.root_path,
                                          self.dataset.location.name,
                                          "temp.{}".format(os.path.basename(
                                              self.dataset.var_filepath(var_config, req_dates))))
        download_path = os.path.join(var_config.root_path,
                                     self.dataset.location.name,
                                     os.path.basename(self.dataset.var_filepath(var_config, req_dates)))
        os.makedirs(os.path.dirname(download_path), exist_ok=True)

        if os.path.exists(download_path):
            logging.warning(f"We have a downloaded file available, skipping: {download_path}")
            return [download_path]

        # TODO: handle time
        request_template = """        retrieve,
          class={product_class},
          date={date},
          {request_args}expver=1,
          levtype={levtype},
          {levlist}param={params},
          method=1,
          origin=ecmf,
          stream={stream},
          system={system},
          time=00:00:00,
          type={product_type},
          area={area},
          grid=0.25/0.25,
          target="{target}",
          format=netcdf
            """
        #          step={step},

        levtype = "plev" if var_config.level else "sfc"

        if self.request_frequency <= Frequency.MONTH:
            req_dates = [dt.replace(day=1) for dt in req_dates]

        request = request_template.format(
            area="/".join([str(s) for s in self.dataset.location.bounds]),
            date="/".join([el.strftime("%Y%m%d") for el in req_dates]),
            levtype=levtype,
            levlist="levelist={},\n          ".format(var_config.level)
            if var_config.level else "",
            params=self.dataset.mars_mapping[var_config.prefix]['param'],
            product_class=self._product_class,
            product_type=self._product_type,
            request_args="".join(["{}={},\n          ".format(k, v) for k, v in self._request_args.items()])
                         if self._request_args is not None else "",
            # TODO: specify - limitations based on access rights to ecmwf data
            step=0,
            stream=self._stream,
            system=self._system,
            target=os.path.basename(temp_download_path),
        )

        if not os.path.exists(temp_download_path):
            logging.debug("MARS REQUEST: \n{}\n".format(request))
            #import sys; sys.exit(0)
            try:
                self._server.execute(request, temp_download_path)
            except ecmwfapi.api.APIException:
                logging.exception("Could not complete ECMWF request: {}")
                return []
        else:
            logging.debug("Already have {}".format(temp_download_path))

        logging.debug("Files downloaded: {}".format(temp_download_path))

        ds = xr.open_dataset(temp_download_path)
        da = getattr(ds, self.dataset.mars_mapping[var_config.prefix]['attribute'])

        if var_config.level:
            da = da.sel(level=int(var_config.level))

        logging.info("Saving MARS file to {}".format(download_path))
        xr_save_netcdf(da, download_path, complevel=self._compress)

        ds.close()

        #if os.path.exists(temp_download_path):
        #    logging.info("Removing {}".format(temp_download_path))
        #    os.unlink(temp_download_path)

        return [download_path]


def mars_main():
    args, request_args = (MARSDownloadArgParser().
                          add_var_specs().
                          add_workers().
                          parse_known_args())
    logging.info("MARS API data downloading")

    location = Location(
        name=args.hemisphere,
        north=args.hemisphere == "north",
        south=args.hemisphere == "south",
    )

    dataset = MARSDataset(
        levels=args.levels,
        location=location,
        var_names=args.vars,
        frequency=getattr(Frequency, args.frequency),
        output_group_by=getattr(Frequency, args.output_group_by),
        config_path=args.config,
        overwrite=args.overwrite_config,
        params=args.params,
        attributes=args.attributes,
    )

    for start_date, end_date in zip(args.start_dates, args.end_dates):
        logging.info("Downloading between {} and {}".format(start_date, end_date))
        mars = MARSDownloader(
            dataset,
            args.product_type,
            args.stream,
            start_date=start_date,
            end_date=end_date,
            max_threads=args.workers,
            request_frequency=getattr(Frequency, args.output_group_by),
            request_args=request_args,
            compress=args.compress,
        )
        mars.download()

        dataset.save_data_for_config(
            rename_var_list={a: v for a, v in zip(args.attributes, args.vars)},
            source_files=mars.files_downloaded,
            var_filter_list=["lambert_azimuthal_equal_area"],
        )

import logging
import os
import requests
import warnings

import numpy as np
import pandas as pd
import xarray as xr

from pyesgf.search import SearchConnection
from pyesgf.logon import LogonManager

from download_toolbox.base import DatasetConfig, Downloader, DataSetError, DownloaderError
from download_toolbox.cli import download_args
from download_toolbox.download import ThreadedDownloader
from download_toolbox.location import Location
from download_toolbox.time import Frequency

"""

"""


class CMIP6DatasetConfig(DatasetConfig):

    DAY_TABLE_MAP = {
        'siconca': 'SI{}',
        'tas': '{}',
        'ta': '{}',
        'tos': 'O{}',
        'hus': '{}',
        'psl': '{}',
        'rlds': '{}',
        'rsus': '{}',
        'rsds': '{}',
        'zg': '{}',
        'uas': '{}',
        'vas': '{}',
        'ua': '{}',
    }

    MONTH_TABLE_MAP = {
        'siconca': 'SI{}',
        'tas': '{}',
        'ta': '{}',
        'tos': 'O{}',
        'hus': '{}',
        'psl': '{}',
        'rlds': '{}',
        'rsus': '{}',
        'rsds': '{}',
        'zg': 'A{}',
        'uas': '{}',
        'vas': '{}',
        'ua': '{}'
    }

    GRID_MAP = {
        'siconca': 'gn',
        'tas': 'gn',
        'ta': 'gn',
        'tos': 'gr',
        'hus': 'gn',
        'psl': 'gn',
        'rlds': 'gn',
        'rsus': 'gn',
        'rsds': 'gn',
        'zg': 'gn',
        'uas': 'gn',
        'vas': 'gn',
        'ua': 'gn',
    }

    def __init__(self, *args,
                 source,
                 member,
                 experiments: object = (
                    "historical",
                    "ssp245",
                 ),
                 default_grid: object = None,
                 grid_override: object = None,
                 identifier=None,
                 table_map_override: object = None,
                 **kwargs):
        super().__init__(*args,
                         identifier="cmip6.{}.{}".format(source, member)
                         if identifier is None else identifier,
                         **kwargs)

        self._experiments = experiments
        self._member = member
        self._source = source

        self._grid_override = dict() if grid_override is None else grid_override
        self._table_map_override = dict() if table_map_override is None else table_map_override

        if type(self._grid_override) is not dict:
            raise DataSetError("Grid override should be a dictionary if supplied")
        if type(self._table_map_override) is not dict:
            raise DataSetError("Table map override should be a dictionary if supplied")

        self._grid_map = CMIP6DatasetConfig.GRID_MAP
        if default_grid is not None:
            self._grid_map = {k: default_grid for k in CMIP6DatasetConfig.GRID_MAP.keys()}
        self._grid_map.update(self._grid_override)

        self._table_map = {k: v.format(self.frequency.cmip_id) for k, v in
                           getattr(CMIP6DatasetConfig, "{}_TABLE_MAP".format(self.frequency.name.upper())).items()}

    @property
    def experiments(self):
        return self._experiments

    @property
    def grid_map(self):
        return self._grid_map

    @property
    def member(self):
        return self._member

    @property
    def source(self):
        return self._source

    @property
    def table_map(self):
        return self._table_map


class CMIP6LegacyDownloader(Downloader):
    # Prioritise European first, US last, avoiding unnecessary queries
    # against nodes further afield (all traffic has a cost, and the coverage
    # of local nodes is more than enough)
    ESGF_NODES = ("aims3.llnl.gov",
                  "esgf.ceda.ac.uk",
                  "esg1.umr-cnrm.fr",
                  "vesg.ipsl.upmc.fr",
                  "esgf3.dkrz.de",
                  "esgf.bsc.es",
                  "esgf-data.csc.fi",
                  "noresg.nird.sigma2.no",
                  "esgf-data.ucar.edu",
                  "esgf-data2.diasjp.net")

    def __init__(self,
                 *args,
                 nodes: object = None,
                 exclude_nodes: object = None,
                 search_node: object = "https://esgf-node.llnl.gov/esg-search/search",
                 **kwargs):
        super().__init__(*args,
                         # TODO: validate ESGF frequencies other than MONTH / YEAR
                         source_min_frequency=Frequency.YEAR,
                         source_max_frequency=Frequency.HOUR,
                         **kwargs)
        exclude_nodes = list() if exclude_nodes is None else exclude_nodes

        self._search_node = search_node
        # self.__connection = None
        self._nodes = nodes if nodes is not None else \
            [n for n in CMIP6LegacyDownloader.ESGF_NODES if n not in exclude_nodes]

    def _single_download(self,
                         var_config: object,
                         req_dates: object) -> list:
        """Overridden CMIP implementation for downloading from DAP server

        Due to the size of the CMIP set and the fact that we don't want to make
        1850-2100 yearly requests for all downloads, we have a bespoke and
        overridden download implementation for this.

        TODO: this could be made to handle much larger date ranges, as individual
         files contain centuries of data. Making a search / download per year isn't ideal

        :param var_config:
        :param req_dates:
        """

        query = {
            'source_id': self.dataset.source,
            'member_id': self.dataset.member,
            'frequency': self.dataset.frequency.cmip_id,
            'variable_id': var_config.prefix,
            'table_id': self.dataset.table_map[var_config.prefix],
            'grid_label': self.dataset.grid_map[var_config.prefix],
        }

        results = []
        # self._connection = SearchConnection(self._search_node, distrib=True)

        for experiment_id in self.dataset.experiments:
            logging.info("Querying ESGF for experiment {} for {}".format(experiment_id, var_config.name))
            query['experiment_id'] = experiment_id
            for data_node in self._nodes:
                query['data_node'] = data_node
                node_results = self.esgf_search(**query)

                if node_results is not None and len(node_results) > 0:
                    logging.debug("Query: {}".format(query))
                    logging.debug("Found {}: {}".format(experiment_id, node_results))
                    results.extend(node_results)
                    break

        if len(results) == 0:
            logging.warning("NO RESULTS FOUND for {} from ESGF search".format(var_config.name))
        else:
            cmip6_da = None
            download_path = os.path.join(var_config.root_path,
                                         self.dataset.location.name,
                                         os.path.basename(self.dataset.var_filepath(var_config, req_dates)))

            logging.debug("\n".join(results))

            try:
                # http://xarray.pydata.org/en/stable/user-guide/io.html?highlight=opendap#opendap
                # Avoid 500MB DAP request limit
                cmip6_da = xr.open_mfdataset(results,
                                             combine='by_coords',
                                             chunks={'time': '499MB'}
                                             )[var_config.prefix]

                start_date, end_date = req_dates[0], req_dates[-1]

                if self.dataset.frequency == Frequency.MONTH and start_date.day > 1:
                    start_date = start_date.replace(day=1)

                cmip6_da = cmip6_da.sel(time=slice(start_date,
                                                   end_date))

                # TODO: possibly other attributes, especially with ocean vars
                if var_config.level:
                    cmip6_da = cmip6_da.sel(plev=int(var_config.level) * 100)

                cmip6_da = cmip6_da.sel(lat=slice(self.dataset.location.bounds[2],
                                                  self.dataset.location.bounds[0]))
            except (OSError, ValueError, IndexError) as e:
                raise DownloaderError("Error encountered: {}".format(e))
            else:
                logging.info("Writing {} to {}".format(cmip6_da, download_path))
                os.makedirs(os.path.dirname(download_path), exist_ok=True)
                cmip6_da.to_netcdf(download_path)
                cmip6_da.close()

            if os.path.exists(download_path):
                return [download_path]
            return [None]

    def esgf_search(self,
                    files_type: str = "OPENDAP",
                    local_node: bool = False,
                    latest: bool = True,
                    project: str = "CMIP6",
                    format: str = "application%2Fsolr%2Bjson",
                    use_csrf: bool = False,
                    **search):
        """

        Below taken from
        https://hub.binder.pangeo.io/user/pangeo-data-pan--cmip6-examples-ro965nih/lab
        and adapted slightly

        :param files_type:
        :param local_node:
        :param latest:
        :param project:
        :param format:
        :param use_csrf:
        :param search:
        :return:
        """
        client = requests.session()
        payload = search
        payload["project"] = project
        payload["type"] = "File"
        if latest:
            payload["latest"] = "true"
        if local_node:
            payload["distrib"] = "false"
        if use_csrf:
            client.get(self._search_node)
            if 'csrftoken' in client.cookies:
                # Django 1.6 and up
                csrftoken = client.cookies['csrftoken']
            else:
                # older versions
                csrftoken = client.cookies['csrf']
            payload["csrfmiddlewaretoken"] = csrftoken

        payload["format"] = format

        offset = 0
        numFound = 10000
        all_files = []
        files_type = files_type.upper()
        while offset < numFound:
            payload["offset"] = offset
            url_keys = []
            for k in payload:
                url_keys += ["{}={}".format(k, payload[k])]

            url = "{}/?{}".format(self._search_node, "&".join(url_keys))
            logging.debug("ESGF search URL: {}".format(url))

            r = client.get(url)
            r.raise_for_status()
            resp = r.json()["response"]
            numFound = int(resp["numFound"])
            resp = resp["docs"]
            offset += len(resp)
            for d in resp:
                for k in d:
                    logging.debug("{}: {}".format(k, d[k]))

                for f in d["url"]:
                    sp = f.split("|")
                    if sp[-1] == files_type:
                        all_files.append(sp[0].split(".html")[0])
        return sorted(all_files)


def main():
    args = download_args(
        dates=True,
        extra_args=[
            (["--source"], dict(type=str, default="MRI-ESM2-0")),
            (["--member"], dict(type=str, default="r1i1p1f1")),
            # (["--pyesgf"], dict(default=False, action="store_true")),
            (("-xs", "--exclude-server"),
             dict(default=[], nargs="*")),
            # (("-o", "--grid-override"), dict(required=None, type=str)),
            (("-g", "--default-grid"), dict(required=None, type=str)),
        ],
        workers=True
    )

    logging.info("CMIP6 Data Downloading")

    location = Location(
        name="hemi.{}".format(args.hemisphere),
        north=args.hemisphere == "north",
        south=args.hemisphere == "south",
    )

    dataset = CMIP6DatasetConfig(
        levels=args.levels,
        location=location,
        member=args.member,
        source=args.source,
        var_names=args.vars,
        frequency=getattr(Frequency, args.frequency),
    )

    # implementation = CMIP6LegacyDownloader if not args.pyesgf else CMIP6PyESGFDownloader
    cmip6 = CMIP6LegacyDownloader(
        dataset=dataset,
        start_date=args.start_date,
        end_date=args.end_date,
        delete_tempfiles=args.delete,
        max_threads=args.workers,
        exclude_nodes=args.exclude_server,
        request_frequency=getattr(Frequency, args.output_group_by),
    )

    logging.info("CMIP downloading: {} {}".format(args.source, args.member))
    cmip6.download()
    dataset.save_data_for_config(
        source_files=cmip6.files_downloaded,
    )


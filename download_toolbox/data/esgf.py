import logging
import os
import requests
import warnings

import numpy as np
import pandas as pd
import xarray as xr

from pyesgf.search import SearchConnection
from pyesgf.logon import LogonManager

from download_toolbox.base import DataSet, Downloader
from download_toolbox.cli import download_args
from download_toolbox.download import ThreadedDownloader
from download_toolbox.location import Location
from download_toolbox.time import DateRequest

"""

"""


class CMIP6DataSet(DataSet):

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

    MON_TABLE_MAP = {
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

        assert type(self._grid_override) is dict, "Grid override should be a dictionary if supplied"
        assert type(self._table_map_override) is dict, "Table map override should be a dictionary if supplied"

        self._grid_map = CMIP6DataSet.GRID_MAP
        if default_grid is not None:
            self._grid_map = {k: default_grid for k in CMIP6DataSet.GRID_MAP.keys()}
        self._grid_map.update(self._grid_override)

        self._table_map = {k: v.format(self.frequency) for k, v in
                           getattr(CMIP6DataSet, "{}_TABLE_MAP".format(self.frequency.upper())).items()}

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


class CMIP6PyESGFDownloader(Downloader):
    """Climate downloader to provide CMIP6 reanalysis data from ESGF APIs

    Useful CMIP6 guidance: https://pcmdi.llnl.gov/CMIP6/Guide/dataUsers.html

    :param identifier: how to identify this dataset
    :param source: source ID in ESGF node
    :param member: member ID in ESGF node
    :param nodes: list of ESGF nodes to query
    :param experiments: experiment IDs to download
    :param frequency: query parameter frequency
    :param table_map: table map for
    :param grid_map:
    :param grid_override:
    :param exclude_nodes:

    "MRI-ESM2-0", "r1i1p1f1", None
    "EC-Earth3", "r2i1p1f1", "gr"

    """

    # HTTP 500 search_node: object = "https://esgf.ceda.ac.uk/esg-search"

    def __init__(self,
                 *args,
                 search_node: object = "https://esgf-data.dkrz.de/esg-search",
                 **kwargs):
        super().__init__(*args, **kwargs)

        self._connection = None
        self._search_node = search_node

        lm = LogonManager()
        lm.logoff()
        lm.is_logged_on()

    def _single_download(self,
                         var_config: object,
                         req_dates: object,
                         download_path: object):
        """Overridden CMIP implementation for downloading from DAP server

        Due to the size of the CMIP set and the fact that we don't want to make
        1850-2100 yearly requests for all downloads, we have a bespoke and
        overridden download implementation for this.

        :param var_prefix:
        :param level:
        :param req_dates:
        """

        var, level = var_config["prefix"], var_config["level"]

        query = {
            'source_id': self.dataset.source,
            'member_id': self.dataset.member,
            'frequency': self.dataset.frequency,
            'variable_id': var,
            'table_id': self.dataset.table_map[var],
            'grid_label': self.dataset.grid_map[var],
        }

        results = []
        self._connection = SearchConnection(self._search_node, distrib=True)

        for experiment_id in self.dataset.experiments:
            logging.info("Querying ESGF for experiment {} for {}".format(experiment_id, var))
            query['experiment_id'] = experiment_id
            ctx = self._connection.new_context(facets="variant_label,data_node", **query)
            ds = ctx.search()[0]
            results = ds.file_context().search()

            if len(results) > 0:
                logging.info("Found {} {} {} results from ESGF search".format(len(results), experiment_id, var))
                results = [f.download_url for f in results]
                break

        if len(results) == 0:
            logging.warning("NO RESULTS FOUND for {} from ESGF search".format(var))
        else:
            cmip6_da = None

            logging.info("\n".join(results))

            try:
                # http://xarray.pydata.org/en/stable/user-guide/io.html?highlight=opendap#opendap
                # Avoid 500MB DAP request limit
                cmip6_da = xr.open_mfdataset(results,
                                             combine='by_coords',
                                             chunks={'time': '499MB'}
                                             )[var]

                cmip6_da = cmip6_da.sel(time=slice(req_dates[0],
                                                   req_dates[-1]))

                # TODO: possibly other attributes, especially with ocean vars
                if level:
                    cmip6_da = cmip6_da.sel(plev=int(level) * 100)

                cmip6_da = cmip6_da.sel(lat=slice(self.dataset.location.bounds[2],
                                                  self.dataset.location.bounds[0]))
            except OSError as e:
                logging.exception("Error encountered: {}".format(e),
                                  exc_info=False)
            else:
                self.save_temporal_files(var_config, cmip6_da)
                cmip6_da.close()

        self._connection.close()


class CMIP6LegacyDownloader(Downloader):
    # Prioritise European first, US last, avoiding unnecessary queries
    # against nodes further afield (all traffic has a cost, and the coverage
    # of local nodes is more than enough)
    ESGF_NODES = ("esgf.ceda.ac.uk",
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
        super().__init__(*args, **kwargs)
        exclude_nodes = list() if exclude_nodes is None else exclude_nodes

        self._search_node = search_node
        self.__connection = None
        self._nodes = nodes if nodes is not None else \
            [n for n in CMIP6LegacyDownloader.ESGF_NODES if n not in exclude_nodes]

    def _single_download(self,
                         var_config: object,
                         req_dates: object,
                         download_path: object):
        """Overridden CMIP implementation for downloading from DAP server

        Due to the size of the CMIP set and the fact that we don't want to make
        1850-2100 yearly requests for all downloads, we have a bespoke and
        overridden download implementation for this.

        :param var_prefix:
        :param level:
        :param req_dates:
        """

        var, level = var_config["prefix"], var_config["level"]

        query = {
            'source_id': self.dataset.source,
            'member_id': self.dataset.member,
            'frequency': self.dataset.frequency,
            'variable_id': var,
            'table_id': self.dataset.table_map[var],
            'grid_label': self.dataset.grid_map[var],
        }

        results = []
        self._connection = SearchConnection(self._search_node, distrib=True)

        for experiment_id in self.dataset.experiments:
            logging.info("Querying ESGF for experiment {} for {}".format(experiment_id, var))
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
            logging.warning("NO RESULTS FOUND for {} from ESGF search".format(var))
        else:
            cmip6_da = None

            logging.info("\n".join(results))

            try:
                # http://xarray.pydata.org/en/stable/user-guide/io.html?highlight=opendap#opendap
                # Avoid 500MB DAP request limit
                cmip6_da = xr.open_mfdataset(results,
                                             combine='by_coords',
                                             chunks={'time': '499MB'}
                                             )[var]

                cmip6_da = cmip6_da.sel(time=slice(req_dates[0],
                                                   req_dates[-1]))

                # TODO: possibly other attributes, especially with ocean vars
                if level:
                    cmip6_da = cmip6_da.sel(plev=int(level) * 100)

                cmip6_da = cmip6_da.sel(lat=slice(self.dataset.location.bounds[2],
                                                  self.dataset.location.bounds[0]))
            except OSError as e:
                logging.exception("Error encountered: {}".format(e),
                                  exc_info=False)
            else:
                self.save_temporal_files(var_config, cmip6_da)
                cmip6_da.close()

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
            (("-xs", "--exclude-server"),
             dict(default=[], nargs="*")),
            (("-o", "--override"), dict(required=None, type=str)),
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

    dataset = CMIP6DataSet(
        levels=args.levels,
        location=location,
        member=args.member,
        source=args.source,
        var_names=args.vars,
        frequency=getattr(DateRequest, args.frequency),
    )

    downloader = CMIP6PyESGFDownloader(
        dataset=dataset,
        start_date=args.start_date,
        end_date=args.end_date,
        delete_tempfiles=args.delete,
        max_threads=args.workers,
        exclude_nodes=args.exclude_server,
        requests_group_by="year",
    )

    logging.info("CMIP downloading: {} {}".format(args.source, args.member))
    downloader.download()

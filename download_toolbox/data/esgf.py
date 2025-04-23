import logging
import os
import requests
import urllib

from functools import lru_cache

import datetime as dt
import xarray as xr

from download_toolbox.dataset import DatasetConfig, DataSetError
from download_toolbox.cli import DownloadArgParser
from download_toolbox.download import ThreadedDownloader, DownloaderError
from download_toolbox.location import Location
from download_toolbox.time import Frequency

"""

"""


class CMIP6DatasetConfig(DatasetConfig):

    DAY_TABLE_MAP = {
        'siconc': 'SI{}',
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
        'siconc': 'SI{}',
        'siconca': 'SI{}',
        'tas': 'A{}',
        'ta': 'A{}',
        'tos': 'O{}',
        'hus': 'A{}',
        'psl': 'A{}',
        'rlds': 'A{}',
        'rsus': 'A{}',
        'rsds': 'A{}',
        'zg': 'A{}',
        'uas': 'A{}',
        'vas': 'A{}',
        'ua': 'A{}'
    }

    GRID_MAP = {
        "EC-Earth3": {
            'siconc': 'gr',
            'siconca': 'gr',
            'tas': 'gr',
            'ta': 'gr',
            'tos': 'gr',
            'hus': 'gr',
            'psl': 'gr',
            'rlds': 'gr',
            'rsus': 'gr',
            'rsds': 'gr',
            'zg': 'gr',
            'uas': 'gr',
            'vas': 'gr',
            'ua': 'gr',
        },
        "MRI-ESM2-0": {
            'siconc': 'gn',
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
    }

    def __init__(self,
                 source: str,
                 member: str,
                 experiments: object = (
                    "historical",
                    "ssp245",
                 ),
                 default_grid: object = None,
                 grid_override: object = None,
                 identifier=None,
                 table_map_override: object = None,
                 **kwargs):
        super().__init__(identifier="cmip6.{}.{}".format(source, member)
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


class CMIP6LegacyDownloader(ThreadedDownloader):
    # Prioritise European first, US last, avoiding unnecessary queries
    # against nodes further afield (all traffic has a cost, and the coverage
    # of local nodes is more than enough)
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
                         # TODO: request_frequency is a problem, we should batch
                         #  as big as possible, but currently only cache queries
                         **kwargs)
        exclude_nodes = list() if exclude_nodes is None else exclude_nodes

        self._search_node = search_node
        # self.__connection = None
        self._exclude_nodes = exclude_nodes if exclude_nodes is not None else []

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
            'grid_label': self.dataset.grid_map[self.dataset.source][var_config.prefix],
        }

        results = []

        logging.info("Querying ESGF for experiment {} for {}".format(" and ".join(self.dataset.experiments), var_config.name))
        query['experiment_id'] = ",".join(self.dataset.experiments)
        node_results = None

        try:
            node_results = self.esgf_search(**query)
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error {e.response} with collecting node results, "
                          f"clearing the cache and maybe try a different search node")
            self.esgf_search.cache_clear()

        if node_results is not None and len(node_results) > 0:
            logging.debug("Query: {}".format(query))
            logging.debug("Found {}-{}: {} results".format(
                ",".join(self.dataset.experiments), var_config.name, len(node_results)))

            start_date, end_date = req_dates[0], req_dates[-1]
            date_proc = lambda s: dt.datetime(int(s[0:4]), int(s[4:6]), 1).date() \
                if self.dataset.frequency <= Frequency.MONTH else dt.datetime(int(s[0:4]), int(s[4:6]),
                                                                              int(s[6:8])).date()
            for idx, df in enumerate(node_results):
                start, end = [date_proc(date_str) for date_str in df.split("_")[-1].rstrip(".nc").split("-")]
                if start <= start_date < end_date <= end \
                        or start <= start_date <= end \
                        or start <= end_date <= end:
                    results.append(df)
        else:
            logging.warning("No unbounded results for {} from ESGF search {}".format(
                var_config.name, ",".join(query.values())))
            return None

        # Filter by excluded domains and group together the rest for testing
        node_grouped_results = dict()
        for result in results:
            host = urllib.parse.urlparse(result).hostname
            if any([excl in host for excl in self._exclude_nodes]):
                logging.debug("Skipping {} as in the excluded hosts list".format(host))
                continue
            if host not in node_grouped_results:
                node_grouped_results[host] = []
            node_grouped_results[host].append(result)

        if len(node_grouped_results) == 0:
            # TODO: what really happens when we have this?
            logging.warning("NO VALID URLs FOUND for {} from ESGF search {}".format(
                var_config.name, ",".join(query.values())))
            return None

        cmip6_ds = None
        download_path = os.path.join(var_config.root_path,
                                     self.dataset.location.name,
                                     os.path.basename(self.dataset.var_filepath(var_config, req_dates)))

        if os.path.exists(download_path):
            logging.warning(f"We have downloaded data without corresponding output, "
                            f"so will skip the download: {download_path}")
            return [download_path]

        logging.debug("\n".join(results))

        for node, grouped_results in node_grouped_results.items():
            logging.info("Attempting to open data from {}".format(node))
            try:
                # http://xarray.pydata.org/en/stable/user-guide/io.html?highlight=opendap#opendap
                # Avoid 500MB DAP request limit
                cmip6_ds = xr.open_mfdataset(grouped_results,
                                             combine='by_coords',
                                             chunks={'time': '499MB'})
            except OSError as e:
                logging.error("Could not open data from {} - {}".format(node, e.filename))
                continue
            break

        if cmip6_ds is None:
            logging.error("We have no URLS that provide data for {} using {}".format(var_config.name, ",".join(query.values())))
            return None

        try:
            rename_vars = {var_config.prefix: var_config.name}
            cmip6_da = getattr(cmip6_ds.rename(rename_vars), var_config.name)

            if self.dataset.frequency == Frequency.MONTH and start_date.day > 1:
                start_date = start_date.replace(day=1)

            cmip6_da = cmip6_da.sel(time=slice(start_date,
                                               end_date))

            # TODO: possibly other attributes, especially with ocean vars
            if var_config.level:
                cmip6_da = cmip6_da.sel(plev=int(var_config.level) * 100)

            cmip6_da = cmip6_da.sel(lat=slice(self.dataset.location.bounds[2],
                                              self.dataset.location.bounds[0]))

            # By this point the variable name has stored this info
            for omit_coord in ["plev", "height"]:
                if omit_coord in cmip6_da.coords:
                    cmip6_da = cmip6_da.drop_vars(omit_coord)
        except (OSError, ValueError, IndexError) as e:
            raise DownloaderError("Error encountered: {} for {}".format(e, results))
        else:
            logging.info("Writing {} to {}".format(cmip6_da, download_path))
            os.makedirs(os.path.dirname(download_path), exist_ok=True)
            cmip6_da.to_netcdf(download_path)
            cmip6_da.close()

        if os.path.exists(download_path):
            return [download_path]

        return None

    @lru_cache(1000)
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
        payload["limit"] = 1000
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
                # for k in d:
                #    logging.debug("{}: {}".format(k, d[k]))

                for f in d["url"]:
                    sp = f.split("|")
                    if sp[-1] == files_type:
                        all_files.append(sp[0].split(".html")[0])

        return sorted(all_files)


def main():
    args = DownloadArgParser().add_var_specs().add_workers().add_extra_args([
        (["--source"], dict(type=str, default="MRI-ESM2-0")),
        (["--member"], dict(type=str, default="r1i1p1f1")),
        (("-n", "--search-node"), dict(type=str, default="https://esgf-node.llnl.gov/esg-search/search")),
        # (["--pyesgf"], dict(default=False, action="store_true")),
        (("-xs", "--exclude-server"),
         dict(default=[], nargs="*")),
        # (("-o", "--grid-override"), dict(required=None, type=str)),
        (("-g", "--default-grid"), dict(required=None, type=str)),
    ]).parse_args()

    logging.info("CMIP6 Data Downloading")

    location = Location(
        name=args.hemisphere,
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
        output_group_by=getattr(Frequency, args.output_group_by),
        config_path=args.config,
        overwrite=args.overwrite_config,
    )

    for start_date, end_date in zip(args.start_dates, args.end_dates):
        logging.info("Downloading between {} and {}".format(start_date, end_date))
        cmip6 = CMIP6LegacyDownloader(
            dataset=dataset,
            start_date=start_date,
            end_date=end_date,
            max_threads=args.workers,
            exclude_nodes=args.exclude_server,
            request_frequency=getattr(Frequency, args.output_group_by),
            search_node=args.search_node,
        )

        logging.info("CMIP downloading: {} {}".format(args.source, args.member))
        cmip6.download()
        dataset.save_data_for_config(
            source_files=cmip6.files_downloaded,
        )

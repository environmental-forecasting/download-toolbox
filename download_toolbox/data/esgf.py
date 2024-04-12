import logging
import os
import warnings

import numpy as np
import pandas as pd
import xarray as xr
from pyesgf.search import SearchConnection

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
        'siconca': 'gn',
        'tas': 'gn',
        'ta': 'gn',
        'tos': 'gr',
        'hus': 'gn',
        'psl': 'gn',
        'rlds': 'gn',
        'rsus': 'gn',   # Surface Upwelling Shortwave Radiation
        'rsds': 'gn',   # Surface Downwelling Shortwave Radiation
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


class CMIP6Downloader(ThreadedDownloader):
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

    # Prioritise European first, US last, avoiding unnecessary queries
    # against nodes further afield (all traffic has a cost, and the coverage
    # of local nodes is more than enough)
    ESGF_NODES = ("esgf.ceda.ac.uk",
                  #"esg1.umr-cnrm.fr",
                  #"vesg.ipsl.upmc.fr",
                  #"esgf3.dkrz.de",
                  "esgf.bsc.es",
                  "esgf-data.csc.fi",
                  "noresg.nird.sigma2.no",
                  #"esgf-data.ucar.edu",
                  #"esgf-data2.diasjp.net",
                  )

    def __init__(self,
                 *args,
                 nodes: object = None,
                 exclude_nodes: object = None,
                 **kwargs):
        super().__init__(*args, **kwargs)

        exclude_nodes = list() if exclude_nodes is None else exclude_nodes

        self.__connection = None
        self._nodes = nodes if nodes is not None else \
            [n for n in CMIP6Downloader.ESGF_NODES if n not in exclude_nodes]

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

        logging.info("Querying ESGF")
        results = []
        self.__connection = SearchConnection("https://esgf-data.dkrz.de/esg-search", distrib=True)

        for experiment_id in self.dataset.experiments:
            query['experiment_id'] = experiment_id

            for data_node in self._nodes:
                query['data_node'] = data_node
                node_results = self.esgf_search(**query)

                if node_results is not None and len(node_results):
                    logging.debug("Query: {}".format(query))
                    logging.debug("Found {}: {}".format(experiment_id,
                                                        node_results))
                    results.extend(node_results)
                    break

        logging.info("Found {} {} results from ESGF search".
                     format(len(results), var))

        cmip6_da = None

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

        self.__connection.close()
        self.save_temporal_files(var_config, cmip6_da)
        cmip6_da.close()

    def additional_regrid_processing(self,
                                     datafile: str,
                                     cube_ease: object):
        """

        :param datafile:
        :param cube_ease:
        """
        (datafile_path, datafile_name) = os.path.split(datafile)
        var_name = datafile_path.split(os.sep)[self._var_name_idx]

        # TODO: regrid fixes need better implementations
        if var_name == "siconca":
            if self._source == 'MRI-ESM2-0':
                cube_ease.data = cube_ease.data / 100.
            cube_ease.data = cube_ease.data.data
        elif var_name in ["tos", "hus1000"]:
            cube_ease.data = cube_ease.data.data

        if cube_ease.data.dtype != np.float32:
            logging.info("Regrid processing, data type not float: {}".
                         format(cube_ease.data.dtype))
            cube_ease.data = cube_ease.data.astype(np.float32)

    def convert_cube(self, cube: object) -> object:
        """Converts Iris cube to be fit for CMIP regrid

        :param cube:   the cube requiring alteration
        :return cube:   the altered cube
        """

        cs = self.sic_ease_cube.coord_system().ellipsoid

        for coord in ['longitude', 'latitude']:
            cube.coord(coord).coord_system = cs
        return cube

    def esgf_search(self, **query):
        # search_server = "https://esgf-node.llnl.gov/esg-search/search"

        query["project"] = "CMIP6"
        ctx = self.__connection.new_context(facets="source", **query)

        # facets=",".join([k for k in query]))

        # query = dict(project="CMIP", source_id="EC-Earth3", member_id="r2i1p1f1",
        # experiment_id="historical", variable="siconca", frequency="SIday", data_node="esgf.ceda.ac.uk")
        logging.debug(query)

        if ctx.hit_count > 0:
            result = ctx.search()[0]
            files = result.file_context().search()
            return [file.opendap_url for file in files]
        return None


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

    downloader = CMIP6Downloader(
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
"""
    logging.info("CMIP regridding: {} {}".format(args.source, args.member))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UserWarning)
        downloader.regrid()
"""

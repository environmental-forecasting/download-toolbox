import copy
import fnmatch
import ftplib
import gzip
import logging
import os

from ftplib import FTP

import datetime as dt
import numpy as np
import pandas as pd
import xarray as xr

from download_toolbox.cli import download_args
from download_toolbox.base import Downloader
from download_toolbox.utils import DaskWrapper


var_remove_list = ["polar_stereographic", "land"]


class AMSRDataSet(DataSet):
    pass

class AMSRDownloader(Downloader):
    """Downloads AMSR2 SIC data from 2012-present using HTTP.

    The data can come from yearly zips, or individual files

    We used to use the following for HTTP downloads:
        - https://seaice.uni-bremen.de/data/amsr2/asi_daygrid_swath/
        - n3125/ or n6250/ for lower or higher resolutions respectively
    But now realise there's anonymous FTP with 3.125km NetCDFs for both hemis
    provided by the University of Hamburg, how kind!

        {'CDI': 'Climate Data Interface version 1.6.5.1 '
                '(http://code.zmaw.de/projects/cdi)',
         'CDO': 'Climate Data Operators version 1.6.5.1 '
                '(http://code.zmaw.de/projects/cdo)',
         'Comment1': 'Scaled land mask value is 12500, NaN values are masked 11500',
         'Comment2': 'After application of scale_factor (multiply with 0.01): land '
                     'mask value is 125, NaN values are masked 115',
         'Conventions': 'CF-1.4',
         'algorithm': 'ASI v5',
         'cite': 'Spreen, G., L. Kaleschke, G. Heygster, Sea Ice Remote Sensing Using '
                 'AMSR-E 89 GHz Channels, J. Geophys. Res., 113, C02S03, '
                 'doi:10.1029/2005JC003384, 2008.',
         'contact': 'alexander.beitsch@zmaw.de',
         'datasource': 'JAXA',
         'description': 'gridded ASI AMSR2 sea ice concentration',
         'geocorrection': 'none',
         'grid': 'NSIDC polar stereographic with tangential plane at 70degN , see '
                 'http://nsidc.org/data/polar_stereo/ps_grids.html',
         'grid_resolution': '3.125 km',
         'gridding_method': 'Nearest Neighbor, with Python package pyresample',
         'hemisphere': 'South',
         'history': 'Tue Nov 11 21:26:36 2014: cdo setdate,2014-11-10 '
                    '-settime,12:00:00 '
                    '/scratch/clisap/seaice/OWN_PRODUCTS/AMSR2_SIC_3125/2014/Ant_20141110_res3.125_pyres_temp.nc '
                    '/scratch/clisap/seaice/OWN_PRODUCTS/AMSR2_SIC_3125/2014/Ant_20141110_res3.125_pyres.nc\n'
                    'Created Tue Nov 11 21:26:35 2014',
         'landmask_value': '12500',
         'missing_value': '11500',
         'netCDF_created_by': 'Alexander Beitsch, alexander.beitsch(at)zmaw.de',
         'offset': '0',
         'sensor': 'AMSR2',
         'tiepoints': 'P0=47 K, P1=11.7 K',
         'title': 'Daily averaged Arctic sea ice concentration derived from AMSR2 L1R '
                  'brightness temperature measurements'}


    :param chunk_size:
    :param dates:
    :param delete_tempfiles:
    :param download:
    :param dtype:
    """
    def __init__(self,
                 *args,
                 **kwargs):
        super().__init__(*args, identifier="amsr2_3125", **kwargs)

    def _single_download(self,
                         var_config: object,
                         req_dates: object,
                         download_path: object):
        raise NotImplementedError("_single_download needs an implementation")


def main():
    args = download_args(var_specs=False,
                         workers=True)

    logging.info("AMSR-SIC Data Downloading")
    sic = AMSRDownloader(
        chunk_size=args.sic_chunking_size,
        dates=[pd.to_datetime(date).date() for date in
               pd.date_range(args.start_date, args.end_date, freq="D")],
        delete_tempfiles=args.delete,
        north=args.hemisphere == "north",
        south=args.hemisphere == "south",
    )
    sic.download()

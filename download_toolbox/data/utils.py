import collections
import logging
import os

import pandas as pd
import xarray as xr


def batch_requested_dates(dates: object,
                          attribute: str = "month") -> object:
    """

    TODO: should be using Pandas DatetimeIndexes / Periods for this, but the
     need to refactor slightly, and this is working for the moment

    :param dates:
    :param attribute:
    :return:
    """
    dates = collections.deque(sorted(dates))

    batched_dates = []
    batch = []

    while len(dates):
        if not len(batch):
            batch.append(dates.popleft())
        else:
            if getattr(batch[-1], attribute) == getattr(dates[0], attribute):
                batch.append(dates.popleft())
            else:
                batched_dates.append(batch)
                batch = []

    if len(batch):
        batched_dates.append(batch)

    if len(dates) > 0:
        raise RuntimeError("Batching didn't work!")

    return batched_dates


def merge_files(new_datafile: str,
                other_datafile: str,
                drop_variables: object = None):
    """

    :param new_datafile:
    :param other_datafile:
    :param drop_variables:
    """
    drop_variables = list() if drop_variables is None else drop_variables

    if other_datafile is not None:
        (datafile_path, new_filename) = os.path.split(new_datafile)
        moved_new_datafile = \
            os.path.join(datafile_path, "new.{}".format(new_filename))
        os.rename(new_datafile, moved_new_datafile)
        d1 = xr.open_dataarray(moved_new_datafile,
                               drop_variables=drop_variables)

        logging.info("Concatenating with previous data {}".format(
            other_datafile
        ))
        d2 = xr.open_dataarray(other_datafile,
                               drop_variables=drop_variables)
        new_ds = xr.concat([d1, d2], dim="time").\
            sortby("time").\
            drop_duplicates("time", keep="first")

        logging.info("Saving merged data to {}... ".
                     format(new_datafile))
        new_ds.to_netcdf(new_datafile)
        os.unlink(other_datafile)
        os.unlink(moved_new_datafile)


def filter_dates_on_data(latlon_path: str,
                         regridded_name: str,
                         req_dates: object,
                         check_latlon: bool = True,
                         check_regridded: bool = True,
                         drop_vars: list = None):
    """Reduces request dates and target files based on existing data

    To avoid what is potentially significant resource expense downloading
    extant data, downloaders should call this method to reduce the request
    dates only to that data not already present. This is a fairly naive
    implementation, in that if the data is present in either the latlon
    intermediate file OR the target regridded file, we'll not bother
    downloading again. This can be overridden via the method arguments.

    :param latlon_path:
    :param regridded_name:
    :param req_dates:
    :param check_latlon:
    :param check_regridded:
    :param drop_vars:
    :return: req_dates(list)
    """

    latlon_dates = list()
    regridded_dates = list()
    drop_vars = list() if drop_vars is None else drop_vars

    # Latlon files should in theory be aggregated and singular arrays
    # meaning we can naively open and interrogate the dates
    if check_latlon and os.path.exists(latlon_path):
        try:
            latlon_dates = xr.open_dataset(
                latlon_path,
                drop_variables=drop_vars).time.values
            logging.debug("{} latlon dates already available in {}".format(
                len(latlon_dates), latlon_path
            ))
        except ValueError:
            logging.warning("Latlon {} dates not readable, ignoring file")

    if check_regridded and os.path.exists(regridded_name):
        regridded_dates = xr.open_dataset(
            regridded_name,
            drop_variables=drop_vars).time.values
        logging.debug("{} regridded dates already available in {}".format(
            len(regridded_dates), regridded_name
        ))

    exclude_dates = list(set(latlon_dates).union(set(regridded_dates)))
    logging.debug("Excluding {} dates already existing from {} dates "
                  "requested.".format(len(exclude_dates), len(req_dates)))

    return sorted(list(pd.to_datetime(req_dates).
                       difference(pd.to_datetime(exclude_dates))))

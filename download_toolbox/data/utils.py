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


def merge_files(new_datafile: object,
                other_datafile: object,
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

        logging.debug("Concatenating with previous data {}".format(
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


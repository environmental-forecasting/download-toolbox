import argparse
import datetime as dt
import re


from download_toolbox.time import Frequency
from download_toolbox.utils import setup_logging

"""

"""


def date_arg(string: str) -> object:
    """

    :param string:
    :return:
    """
    date_match = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", string)
    return dt.date(*[int(s) for s in date_match.groups()])


def dates_arg(string: str) -> object:
    """

    :param string:
    :return:
    """
    if string == "none":
        return []

    date_match = re.findall(r"(\d{4})-(\d{1,2})-(\d{1,2})", string)

    if len(date_match) < 1:
        raise argparse.ArgumentError(argument="dates",
                                     message="No dates found for supplied argument {}".format(string))
    return [dt.date(*[int(s) for s in date_tuple]) for date_tuple in date_match]


def csv_arg(string: str) -> list:
    """

    :param string:
    :return:
    """
    csv_items = []
    string = re.sub(r'^\'(.*)\'$', r'\1', string)

    for el in string.split(","):
        if len(el) == 0:
            csv_items.append(None)
        else:
            csv_items.append(el)
    return csv_items


def csv_of_csv_arg(string: str) -> list:
    """

    :param string:
    :return:
    """
    csv_items = []
    string = re.sub(r'^\'(.*)\'$', r'\1', string)

    for el in string.split(","):
        if len(el) == 0:
            csv_items.append(None)
        else:
            csv_items.append(el.split("|"))
    return csv_items


def int_or_list_arg(string: str) -> object:
    """

    :param string:
    :return:
    """
    try:
        val = int(string)
    except ValueError:
        val = string.split(",")
    return val


@setup_logging
def download_args(choices: object = None,
                  dates: bool = True,
                  dates_optional: bool = False,
                  frequency: bool = True,
                  var_specs: bool = True,
                  workers: bool = False,
                  extra_args: object = ()) -> object:
    """

    :param choices:
    :param dates:
    :param dates_optional:
    :param frequency:
    :param var_specs:
    :param workers:
    :param extra_args:
    :return:
    """

    ap = argparse.ArgumentParser()
    ap.add_argument("hemisphere", choices=("north", "south"))

    if choices and isinstance(choices, list):
        ap.add_argument("-c", "--choice", choices=choices, default=choices[0])

    if dates:
        pos_args = [["start_date"], ["end_date"]] if not dates_optional else \
            [["-sd", "--start-date"], ["-ed", "--end-date"]]
        ap.add_argument(*pos_args[0], type=date_arg, default=None)
        ap.add_argument(*pos_args[1], type=date_arg, default=None)

    freq_avail = [_.name for _ in list(Frequency)]

    if frequency:
        ap.add_argument("-f", "--frequency",
                        choices=freq_avail,
                        default=freq_avail[-1])

    ap.add_argument("-o", "--output-group-by",
                    choices=freq_avail,
                    default=freq_avail[0])
    ap.add_argument("-oc", "--overwrite-config",
                    help="Overwrite dataset configuration",
                    action="store_true", default=False)

    if workers:
        ap.add_argument("-w", "--workers", default=8, type=int)

    ap.add_argument("-p", "--parallel-opens",
                    default=False, action="store_true",
                    help="Allow xarray mfdataset to work with parallel opens")

    ap.add_argument("-d", "--dont-delete", dest="delete",
                    action="store_false", default=True)
    ap.add_argument("-v", "--verbose", action="store_true", default=False)

    if var_specs:
        ap.add_argument("vars",
                        help="Comma separated list of vars",
                        type=csv_arg,
                        default=[])
        ap.add_argument("levels",
                        help="Comma separated list of pressures/depths as needed, "
                             "use zero length string if None (e.g. ',,500,,,') and "
                             "pipes for multiple per var (e.g. ',,250|500,,'",
                        type=csv_of_csv_arg,
                        default=[])

    for arg in extra_args:
        ap.add_argument(*arg[0], **arg[1])
    args = ap.parse_args()

    if var_specs:
        assert len(args.vars) > 0 and len(args.vars) == len(args.levels), \
            "You must specify variables and levels of equal length, >=1"

    return args

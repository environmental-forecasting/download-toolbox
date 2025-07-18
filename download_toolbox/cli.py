import argparse
import datetime as dt
import logging
import re

from download_toolbox.time import Frequency

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


def csv_of_date_args(string: str) -> list:
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
            csv_items.append([date_arg(date) for date in el.split("|")])
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


def parse_known_args(unknown_args: list) -> dict:
    """
    Parses a list of unknown command line arguments and returns them.

    This function processes both long and short options, and supports boolean flags
    (e.g., `--flag` or `-f`). Non-option arguments are added to the dictionary as
    values associated with their preceding option keys.

    Args:
        unknown_args: A list of command line arguments.

    Returns:
        A dictionary containing parsed arguments.
        Keys are converted to snake_case for consistency,
        with how argparse would return them.
    """
    result = {}
    key = None

    for arg in unknown_args:
        if arg.startswith("--"):
            # Cover long option
            if key:
                result[key] = True
            key = arg.lstrip("-").replace("-", "_")
        elif len(arg) > 1 and arg.startswith("-"):
            # Cover short option
            key = arg.replace("-", "_")
            result[key] = True
        else:
            if key:
                result[key] = arg
                key = None
            else:
                pass

    if key:
        result[key] = True

    return result


class BaseArgParser(argparse.ArgumentParser):
    """An ArgumentParser specialised to support common argument handling

    The 'allow_*' methods return self to permit method chaining.

    :param suppress_logs:
    """

    def __init__(self,
                 *args,
                 log_format: str = "[%(asctime)-17s :%(levelname)-8s] - %(message)s",
                 suppress_logs: list = None,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self._log_format = log_format
        self._suppress_logs = suppress_logs

        # FIXME: this is not ubiquitously useful in preprocess-toolbox: ref channel adds
        #   so there is a pattern failure having it at this level
        self.add_argument("-c", "--config-path",
                          dest="config",
                          help="Path at which to output the configuration when rendered")

        self.add_argument("-v",
                          "--verbose",
                          action="store_true",
                          default=False)

    def add_extra_args(self, extra_args):
        for arg in extra_args:
            self.add_argument(*arg[0], **arg[1])
        return self

    def parse_args(self,
                   *args,
                   **kwargs):
        args = super().parse_args(*args, **kwargs)

        self.setup_logging(verbose=args.verbose)

        return args

    def parse_known_args(self,
                   *args,
                   **kwargs):
        """
        Parses command line arguments and handles unknown arguments separately.

        This method first calls the argparse's `parse_known_args` method to separate
        known and unknown arguments. It then parses the unknown arguments using
        the `parse_known_args` function to convert the returned list to a dict.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            A tuple containing the parsed Namespace object for known arguments
            and a dictionary of parsed unknown arguments.
        """
        args, unknown_args_list = super().parse_known_args(*args, **kwargs)

        unknown_args = parse_known_args(unknown_args_list)

        self.setup_logging(verbose=args.verbose)

        return args, unknown_args

    def setup_logging(self, verbose: bool) -> None:
        """
        Configures logging based on verbosity and suppresses logs from specific modules.

        Args:
            verbose: Whether to enable debug-level logging.
        """
        # TODO: this is not necessarily ideal when running the argparser in notebooks
        loglevel = logging.DEBUG if verbose else logging.INFO
        logging.basicConfig(
            datefmt="%d-%m-%y %T",
            format=self._log_format,
            level=loglevel
        )
        logging.getLogger().setLevel(loglevel)

        if self._suppress_logs is not None and type(self._suppress_logs) is list:
            for log_module in self._suppress_logs:
                logging.debug("Setting {} to WARNING only".format(log_module))
                logging.getLogger(log_module).setLevel(logging.WARNING)

        # TODO: bring these out of defaults
        logging.getLogger("cdsapi").setLevel(logging.WARNING)
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("matplotlib").setLevel(logging.WARNING)


class DownloadArgParser(BaseArgParser):
    def __init__(self,
                 *args,
                 dates_optional: bool = False,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self._var_specs = False

        self.add_argument("hemisphere", choices=("north", "south"))

        date_arg_ids = [["start_dates"], ["end_dates"]] if not dates_optional else \
            [["-sd", "--start-dates"], ["-ed", "--end-dates"]]
        self.add_argument(*date_arg_ids[0], type=dates_arg, default=None)
        self.add_argument(*date_arg_ids[1], type=dates_arg, default=None)

        freq_avail = [_.name for _ in list(Frequency)]

        self.add_argument("-f", "--frequency",
                          choices=freq_avail,
                          default=freq_avail[-1])
        self.add_argument("-o", "--output-group-by",
                          choices=freq_avail,
                          default=freq_avail[0])

        self.add_argument("-oc", "--overwrite-config",
                          help="Overwrite dataset configuration",
                          action="store_true", default=False)

    def add_var_specs(self):
        self._var_specs = True

        self.add_argument("vars",
                          help="Comma separated list of vars",
                          type=csv_arg,
                          default=[])
        self.add_argument("levels",
                          help="Comma separated list of pressures/depths as needed, "
                               "use zero length string if None (e.g. ',,500,,,') and "
                               "pipes for multiple per var (e.g. ',,250|500,,'",
                          type=csv_of_csv_arg,
                          default=[])

        return self

    def add_workers(self):
        self.add_argument("-w", "--workers", default=8, type=int)
        return self

    def parse_args(self,
                   *args,
                   **kwargs):
        args = super().parse_args(*args, **kwargs)

        if self._var_specs:
            if not (len(args.vars) > 0 and len(args.vars) == len(args.levels)):
                raise RuntimeError("You must specify variables and levels of equal length, >=1: {} != {}".
                                   format(args.vars, args.levels))
        return args


class CDSDownloadArgParser(DownloadArgParser):
    def __init__(self,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.add_argument("-i", "--identifier",
                          help="",
                          default="cds",
                          type=str)

    def add_cds_specs(self):
        """Arguments for dataset and product_type"""
        self.add_argument("-ds", "--dataset",
                          help="Dataset to download",
                          type=str)
        self.add_argument("-pt", "--product-type",
                          help="Product type for the dataset",
                          type=str)
        self.add_argument("--time",
                          help="Comma separated list of times for the dataset ('00:00,01:00'...), or 'all' for all 24 hours",
                          type=csv_arg,
                          default=[])

        # TODO: Pull this to constructor and update other downloaders
        self.add_argument("--compress",
                          help="Provide an integer from 1-9 (low to high) on how much to compress the output netCDF",
                          default=None,
                          type=int)

        return self

    def add_derived_specs(self):
        """Arguments for derived datasets"""
        self.add_argument("--daily-statistic",
                          help="Daily statistic for derived datasets",
                          type=str,
                          default="daily_mean")
        self.add_argument("--time-zone",
                          help="Time zone for derived datasets",
                          type=str,
                          default="utc+00:00")
        self.add_argument("--derived-frequency",
                          help="Frequency for derived datasets",
                          type=str,
                          default="1_hourly")

        return self


class AWSDownloadArgParser(DownloadArgParser):
    """Arguments for AWS datasets"""
    def __init__(self,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)


    def add_aws_specs(self):
        self.add_argument("--delete-cache",
                          help="Delete raw source download cached files after saving output netCDFs",
                          default=False,
                          action=argparse.BooleanOptionalAction)
        self.add_argument("--cache-only",
                          help="Only download the source files into the filecache, do nothing else",
                          default=False,
                          action=argparse.BooleanOptionalAction)

        # TODO: Pull this to constructor and update other downloaders
        self.add_argument("--compress",
                          help="Provide an integer from 1-9 (low to high) on how much to compress the output netCDF",
                          default=None,
                          type=int)

        return self

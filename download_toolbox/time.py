from enum import Enum, auto


class Frequency(int, Enum):
    """

    https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#period-aliases

    """
    YEAR = 1, "%Y", "Y", "year", "yr"
    MONTH = 2, "%Y%m", "M", "month", "mon"
    DAY = 3, "%Y%m%d", "D", "day", "day"
    HOUR = 4, "%Y%m%d%h", "H", "hour", "hr"

    def __new__(cls, value, date_format, freq, attribute, cmip_id):
        member = int.__new__(cls, value)
        member._value_ = value
        member.date_format = date_format
        member.freq = freq
        member.attribute = attribute
        member.cmip_id = cmip_id
        return member


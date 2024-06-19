from enum import Enum, auto


class Frequency(int, Enum):
    """

    https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#period-aliases

    """
    YEAR = 1, "%Y", "Y", "year"
    MONTH = 2, "%Y%m", "ME", "month"
    DAY = 3, "%Y%m%d", "D", "day"

    def __new__(cls, value, date_format, freq, attribute):
        member = int.__new__(cls, value)
        member._value_ = value
        member.date_format = date_format
        member.freq = freq
        member.attribute = attribute
        return member


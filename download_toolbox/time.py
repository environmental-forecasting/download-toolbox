from enum import Enum, auto


# TODO: review enum usage, especially in light of varying python versions which
#  change the enum modules facilities a lot
class Frequency(int, Enum):
    YEAR = 1, "%Y", "Y", "year"
    MONTH = 2, "%Y%m", "M", "month"
    DAY = 3, "%Y%m%d", "D", "date"

    def __new__(cls, value, date_format, freq, attribute):
        member = int.__new__(cls, value)
        member._value_ = value
        member.date_format = date_format
        member.freq = freq
        member.attribute = attribute
        return member


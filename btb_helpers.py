import logging
from datetime import datetime
import dateparser
import pytz
import time
import functools
import mplfinance as mpf
import pandas as pd

logger = logging.getLogger(__name__)

def interval_to_milliseconds(interval):
    """Convert a Binance interval string to milliseconds
    1 jan 1970 was thursday, I found this out too late, so the program don't accept 1w interwals
    :param interval: Binance interval string 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d
    :type interval: str
    :return:
         None if unit not one of m, h, d
         None if string not in correct format
         int value of interval in milliseconds
    """
    ms = None
    seconds_per_unit = {
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
    }

    unit = interval[-1]
    if unit in seconds_per_unit:
        try:
            ms = int(interval[:-1]) * seconds_per_unit[unit] * 1000
        except ValueError:
            logger.error(f'interval_to_milliseconds got invalid interval[:-1],'
                         f' was expecting int, got {interval[:-1]}')
    else:
        logger.error(f'interval_to_milliseconds got invalid interval unit {unit},'
                     f'valid interval units are: m, h, d')
    return ms


def date_to_milliseconds(date_str):
    """Convert UTC date to milliseconds
    If using offset strings add "UTC" to date string e.g. "now UTC", "11 hours ago UTC"
    See dateparse docs for formats http://dateparser.readthedocs.io/en/latest/
    :param date_str: date in readable format, i.e. "January 01, 2018", "11 hours ago UTC", "now UTC"
    :type date_str: str
    """
    # get epoch value in UTC
    epoch = datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)
    # parse our date string
    d = dateparser.parse(date_str)
    # if the date is not timezone aware apply UTC timezone
    if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
        d = d.replace(tzinfo=pytz.utc)

    # return the difference in time
    return int((d - epoch).total_seconds() * 1000.0)

def span_to_list(span: tuple, n_points) -> list:
    """
    transforms a range(span) into a list of n points evenly distributed within the range
    :param span: the range like (0, 10), both ends included
    :param n_points: number of points
    :return: list of points
    >>> span_to_list((0, 10), 5)
    [0, 2, 5, 8, 10]
    """
    point_list = [span[0]]
    section_len = (span[1] - span[0])/(n_points - 1)
    for i in range(n_points - 1):
        point_list.append(point_list[-1] + section_len)
    return [round(x) for x in point_list]
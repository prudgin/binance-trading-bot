from abc import ABCMeta, abstractmethod
import collections

import pandas as pd
import mplfinance as mpf
import talib

import helper_functions as hlp


class Indicator(object):
    """
    Indicator is an abstract base class providing an interface for
    all subsequent (inherited) indicator objects.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def calculate_next(self):
        """
        Provides the mechanisms to calculate the list of signals.
        """
        raise NotImplementedError("Should implement calculate_indicator()")


class EMA(Indicator):
    """
    Exponential moving average indicator
    """

    def __init__(self, n):
        self.n = n
        self.last_entry = None

    def calculate_next(self, data_feed: dict) -> collections.deque:
        #  get newest candle, append ema values to self buffer
        #  do I really need those bufers?

        alpha = 2 / (self.n + 1)
        if self.last_entry:
            ema = alpha * data_feed['close'] + (1 - alpha) * self.last_entry['ema']

        else:
            ema = data_feed['close']

        self.last_entry = {
            'open_time': data_feed['open_time'],
            'ema': ema
        }

        return self.last_entry



        






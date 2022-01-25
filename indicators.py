from abc import ABCMeta, abstractmethod

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
    def calculate_indicator(self):
        """
        Provides the mechanisms to calculate the list of signals.
        """
        raise NotImplementedError("Should implement calculate_indicator()")


class EMA(Indicator):
    """
    Exponential moving average indicator
    """

    def __init__(self):
        pass


    def calculate_indicator(self, n, bars: list):
        self.n = n
        print(bars)
        self.bars = pd.DataFrame(bars)
        assert len(self.bars) >= n

        self.alpha = 2/(self.n + 1)


        #prepared = hlp.prepare_df_for_plotting(self.bars)
        #mpf.plot(prepared, type='candle')
        






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
    def fill_buffer(self):
        """
        Provides the mechanisms to calculate the list of signals.
        """
        raise NotImplementedError("Should implement calculate_indicator()")


class EMA(Indicator):
    """
    Exponential moving average indicator
    """

    def __init__(self, n, max_buffer_size):
        self.n = n
        self.buffer = collections.deque(maxlen=max_buffer_size)



    def fill_buffer(self, data_feed: collections.deque) -> collections.deque:
        # here we suppose that data_feed is indexed by time and uniform. Need to assert this.
        data_feed = list(data_feed)  # ve have a list of dicts now

        alpha = 2 / (self.n + 1)

        if not self.buffer:
            last = 0
        else:
            last = self.buffer[-1]['open_time']

        #  most of the times data_feed is only 1 bar ahead, check it
        if ((len(data_feed) >= 2 and len(self.buffer) >= 1) and
                data_feed[-2]['open_time'] == self.buffer[-1]['open_time']):
            ema = alpha * data_feed[-1]['close'] + (1 - alpha) * self.buffer[-1]['ema']
            self.buffer.append({
                'open_time': data_feed[-1]['open_time'],
                'ema': ema
            })
        else:
            #  self.buffer is more than 1 block behind data_feed, need to iterate
            for bar in data_feed:
                if bar['open_time'] > last:
                    if last == 0:
                        ema = bar['close']
                    else:
                        ema = alpha * bar['close'] + (1 - alpha) * self.buffer[-1]['ema']
                    self.buffer.append({
                        'open_time': bar['open_time'],
                        'ema': ema
                    })



        






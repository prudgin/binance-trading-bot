from abc import ABCMeta, abstractmethod
import collections
import time

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import mplfinance as mpf
from historical_data import get_hist_data as ghd
import events
import historical_data.helper_functions as hlp

class DataBuffer():
    """
    This buffer is used to store data as a log during backtest run or live trading.
    In case of live trading it should save itself to a file or database from time to time.
    The buffer itself is a dict of dicts. Outer dict keys are timestamps, inner dicts contain the data.
    """

    def __init__(self, symbol, max_size=None):
        """
        :param max_size: maximum length of buffer in MB?
        """
        self.buffer = {}
        self.symbol = symbol

    def append_data(self, new_data: dict):
        """
        :param new_data: dict with timestamp key, named 'close_time' (time of closure of the last candle)
        :return:
        """
        # TODO new_data can be a list of dicts
        start = time.perf_counter()

        new_data_time = new_data['close_time']

        if new_data_time in self.buffer.keys():
            assert self.buffer[new_data_time]['close_time'] == new_data_time, 'buffer timings collision'
            self.buffer[new_data_time] = {**self.buffer[new_data_time], **new_data}

        else:
            self.buffer[new_data_time] = new_data


    def get_len(self):
        return len(self.buffer)

    def get_item_by_timestamp(self, timestamp):
        keys = self.buffer.keys()
        if timestamp in keys:
            return self.buffer[timestamp]
        else:
            prev_timestamp = max([i for i in keys if i < timestamp])
            return self.buffer[prev_timestamp]

    def get_all_data(self) -> pd.DataFrame:
        df = pd.DataFrame.from_dict(self.buffer, orient='index')
        df.index = pd.to_datetime(df.index, unit='ms')
        df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
        return df

    def feed_param_names(self, *args):
        self.params = [str(i) for i in args]

    def draw(self):

        cols_list = ['total', 'open', 'high', 'low', 'close', 'volume',
                     'price_filled', 'close_time', self.symbol, *self.params]
        df = self.get_all_data()[cols_list]

        df['up'] = df['price_filled'].loc[df[self.symbol] > 0]
        df['down'] = df['price_filled'].loc[df[self.symbol] < 0]

        apds = [mpf.make_addplot(df[self.params]),
                mpf.make_addplot(df['up'], type='scatter', markersize=100, marker='^', color='green'),
                mpf.make_addplot(df['down'], type='scatter', markersize=100, marker='v', color='red'),
                mpf.make_addplot(df['total'], secondary_y=True, color='brown')
        ]

        fig, axlist = mpf.plot(
            df[['open', 'high', 'low', 'close', 'volume']],
            type='candle',
            volume=True,
            addplot = apds,
            returnfig=True
        )
        axlist[1].legend(['balance total'])


        plt.show()
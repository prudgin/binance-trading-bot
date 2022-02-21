from abc import ABCMeta, abstractmethod
import collections
import time

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import mplfinance as mpf
import events


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
        if not len(self.buffer):
            return None
        df = pd.DataFrame.from_dict(self.buffer, orient='index')
        df.index = pd.to_datetime(df.index, unit='ms')
        df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
        return df

    def get_params(self):
        return self.params

    def get_symbol(self):
        return self.symbol

    def feed_param_names(self, *args):
        self.params = [str(i) for i in args]

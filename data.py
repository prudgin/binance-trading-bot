from abc import ABCMeta, abstractmethod
import collections
import time

import pandas as pd
import matplotlib.pyplot as plt
import spooky
from binance_sql import historical_data as ghd
import events


class DataHandler(object):
    """
    DataHandler is an abstract base class providing an interface for
    all subsequent (inherited) data handlers (both live and historic).
    The goal of a (derived) DataHandler object is to output a generated
    set of bars (OLHCVI) for each symbol requested.
    This will replicate how a live strategy would function as current
    market data would be sent "down the pipe". Thus a historic and live
    system will be treated identically by the rest of the backtesting suite.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_buffered_bars(self, symbol):
        """
        Returns the last N bars from the buffered_data queue,
        """
        raise NotImplementedError("Should implement get_latest_bars()")

    @abstractmethod
    def update_bars(self):
        """
        Pushes the latest bar to the latest symbol structure
        for all symbols in the symbol list.
        """
        raise NotImplementedError("Should implement update_bars()")


class HistoricDataHandler(DataHandler):
    """
    HistoricDataHandler is designed to read historic data for
    each requested symbol and provide an interface
    to obtain the "latest" bar in a manner identical to a live
    trading interface.
    """

    def __init__(self, events, buffer, symbol, interval, start_ts, end_ts, delete_previous_data=False,
                 imported_data=False):
        """
        Initialises the historic data handler.
        :param events: the events queue
        :param symbol: symbol in interest
        :param interval: Binance interval string 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d
        :param start_ts: backtester start timestamp
        :param end_ts: backtester stop timestamp
        :param latest_data_maxlen: max length of latest_symbol_data, where we append new candles
        :param import_data: download or import data
        """

        self.events = events
        self.buffer = buffer
        self.symbol = symbol
        self.interval = interval
        self.start_ts = start_ts
        self.end_ts = end_ts
        self.delete_prev_data = delete_previous_data

        self.historical_data = []
        self.continue_backtest = True

        if imported_data:
            self.import_historical_data(imported_data)
        else:
            self.load_historical_data()

    def load_historical_data(self):
        #  returns a list of tuples, each representing a candle, returned in desc order, so we can pop from list

        conn_creds = {
            'host': spooky.creds['host'],
            'user': spooky.creds['user'],
            'password': spooky.creds['password'],
            'database': spooky.creds['database']
        }
        candle_getter = ghd.data_manager(self.symbol, self.interval)
        candle_getter.set_database_credentials(**conn_creds)
        self.historical_data = candle_getter.get_candles(start_ts=self.start_ts, end_ts=self.end_ts,
                                                         delete_existing_table=self.delete_prev_data,
                                                         reversed_order=True)
        if not self.historical_data:
            self.continue_backtest = False

    def import_historical_data(self, data: list):
        """
        :param data: a list of tuples, each representing a candle, organised in desc order, so we can pop from list
        :return:
        """
        self.historical_data = data
        if not self.historical_data:
            self.continue_backtest = False

    def _pop_new_bar(self) -> dict:
        """
        :return: Dict. The latest bar from the data feed.
        """
        new_raw_bar = self.historical_data.pop()
        #  if list is empty after pop, don't go on the nex loop
        if not self.historical_data:
            self.continue_backtest = False
        keys = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                'quote_vol', 'num_trades', 'buy_base_vol', 'buy_quote_vol']
        new_bar = dict(zip(keys, new_raw_bar))
        if new_bar['open'] < 0:
            # print('missing bar detected!!!!!!!!!!=========================')
            return None
        else:
            return new_bar

        # should think of something in case of recieving multiple bars while live trading

    def update_bars(self):
        """
        Pushes the latest bar to the buffered_data queue.
        """
        #  TODO: in live trading new data can be a bunch of canldes if
        #   for example we download them after restoring lost connection to exchange
        new_data = self._pop_new_bar()
        if new_data:
            self.buffer.append_data(new_data)
            self.events.put(events.MarketEvent(new_data))

from abc import ABCMeta, abstractmethod
import pandas as pd
import get_hist_data as ghd
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
    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or fewer if less bars are available.
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

    def __init__(self, events, symbol, interval, start_ts, end_ts):
        """
        Initialises the historic data handler.
        :param events: the events queue
        :param symbol: symbol in interest
        :param interval: Binance interval string 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d
        :param start_ts: backtester start timestamp
        :param end_ts: backtester stop timestamp
        """

        self.events = events
        self.symbol = symbol
        self.interval = interval
        self.start_ts = start_ts
        self.end_ts = end_ts

        self.symbol_data = {}
        self.latest_symbol_data = []
        self.continue_backtest = True
        self.load_historical_data()

    def load_historical_data(self):
        #  returns a list of tuples, each representing a candle, returned in desc order, so we can pop from list
        self.symbol_data = ghd.get_candles_from_db(self.symbol, self.interval, self.start_ts, self.end_ts)

    def _get_new_bar(self):
        """
        Returns the latest bar from the data feed as a tuple of
        (id, open_time, open, high, low, close, volume, close_time,
         quote_vol, num_trades, buy_base_vol, buy_quote_vol, ignored, time_loaded).
        """
        return self.symbol_data.pop()

    def update_bars(self):
        """
        Pushes the latest bar to the latest_symbol_data structure.
        """
        if self.symbol_data:
            bar = self._get_new_bar()
            self.latest_symbol_data.append(bar)
        else:
            self.continue_backtest = False
        self.events.put(events.MarketEvent())

    def get_latest_bars(self, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or N-k if less available.
        """
        if len(self.latest_symbol_data) >= N:
            return self.latest_symbol_data[-N:]
        else:
            return None

from abc import ABCMeta, abstractmethod
import math
import collections
import time

import indicators
import events


class Strategy(object):
    """
    Strategy is an abstract base class providing an interface for
    all subsequent (inherited) strategy handling objects.

    The goal of a (derived) Strategy object is to generate Signal
    objects for particular symbols based on the inputs of Bars
    (OLHCVI) generated by a DataHandler object.

    This is designed to work both with historic and live data as
    the Strategy object is agnostic to the data source,
    since it obtains the bar tuples from a queue object.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def calculate_signals(self):
        """
        Provides the mechanisms to calculate the list of signals.
        """
        raise NotImplementedError("Should implement calculate_signals()")


class EMAStrategy(Strategy):
    """
    This is an extremely simple strategy
    """

    def __init__(self, events, symbol, fast, slow, max_buffer_size):
        """
        Initialises the EMA strategy.
        """
        self.threshold = 0  # TODO add threshold for how small ema difference should be treated as a signal
        self.symbol = symbol
        self.events = events
        self.buffer = collections.deque(maxlen=max_buffer_size)
        self.state = 0  # are we long, short or out of market? [-1, 0, 1]

        self.ema_fast = indicators.EMA(fast)
        self.ema_slow = indicators.EMA(slow)

    def calculate_signals(self, event: events.MarketEvent):
        #  TODO: in live trading data feed can be a bunch of canldes, not just one
        #   if for example we download them after restoring lost connection to exchange

        data_feed = event.new_data

        slow_name = self.ema_slow.name
        fast_name = self.ema_fast.name

        self.ema_fast.calculate_next(data_feed)
        self.ema_slow.calculate_next(data_feed)
        self.buffer.append({
            'open_time': data_feed['open_time'],
            fast_name: self.ema_fast.last_entry,
            slow_name: self.ema_slow.last_entry,
        })
        signal_fired = 0
        #  ema is an unstable function, so we can act only after the period of instability has passed
        if len(self.buffer) > self.ema_slow.n:
            curr_diff = self.buffer[-1][fast_name] - self.buffer[-1][slow_name]
            prev_diff = self.buffer[-2][fast_name] - self.buffer[-2][slow_name]
            sign = lambda x: math.copysign(1, x) if x else 0
            curr_sign, prev_sign = sign(curr_diff), sign(prev_diff)

            if prev_sign > 0 and curr_sign < 0:
                self.state = -1
                signal_fired = ['EXIT', 'SHORT']
            if prev_sign < 0 and curr_sign > 0:
                self.state = 1
                signal_fired = ['EXIT', 'LONG']
            if prev_sign == 0:
                if curr_sign > 0 and self.state <= 0:
                    self.state = 1
                    signal_fired = ['EXIT', 'LONG']
                if curr_sign < 0 and self.state >= 0:
                    self.state = -1
                    signal_fired = ['EXIT', 'SHORT']

        if signal_fired:
            self.events.put(
                events.SignalEvent(
                    self.symbol,
                    data_feed['close_time'],
                    signal_fired,
                    data_feed['close']
                )
            )

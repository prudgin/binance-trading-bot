from abc import ABCMeta, abstractmethod
from math import floor
import copy
import time
import datetime
import numpy as np
import pandas as pd
import queue
import events


class Portfolio(object):
    """
    The Portfolio class handles the positions and market
    value of all instruments at a resolution of a "bar",
    i.e. secondly, minutely, 5-min, 30-min, 60 min or EOD.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def update_signal(self, event):
        """
        Acts on a SignalEvent to generate new orders
        based on the portfolio logic.
        """
        raise NotImplementedError("Should implement update_signal()")

    @abstractmethod
    def update_fill(self, event):
        """
        Updates the portfolio current positions and holdings
        from a FillEvent.
        """
        raise NotImplementedError("Should implement update_fill()")


class NaivePortfolio(Portfolio):
    """
    The NaivePortfolio object is designed to send orders to
    a brokerage object with a constant quantity size blindly,
    i.e. without any risk management or position sizing. It is
    used to test simpler strategies such as BuyAndHoldStrategy.
    """

    def __init__(self, events, buffer, symbol, initial_capital, bet_size, start_ts):
        """
        Initialises the portfolio with bars and an event queue.
        Also includes a starting datetime index and initial capital
        (USD unless otherwise stated).

        Parameters:
        bars - The DataHandler object with current market data.
        events - The Event Queue object.
        start_date - The start date (bar) of the portfolio.
        initial_capital - The starting capital in USD.
        bet_size - part of available balance to put per trade
        """
        self.symbol = symbol
        self.events = events
        self.buffer = buffer
        self.start_date = start_ts
        self.initial_capital = initial_capital
        self.bet_size = bet_size

        self.current_position = {self.symbol: 0}

        self.current_holdings = {
            self.symbol: 0,
            'cash': self.initial_capital,
            'commission': 0,
            'total': self.initial_capital
        }

    def update_timeindex(self, event: events.MarketEvent):
        """
        Adds a new record to the positions matrix for the current
        market data bar. This reflects the PREVIOUS bar, i.e. all
        current market data at this stage is known (OLHCVI).
        Makes use of a MarketEvent from the events queue.
        """
        new_data = event.new_data

        self.current_holdings['total'] = (
                self.current_holdings['cash'] +
                self.current_holdings[self.symbol] * new_data['close']
            # TODO new_data can be a list of dicts!
        )

        self.buffer.append_data({
            'close_time': new_data['close_time'],
            self.symbol: self.current_holdings[self.symbol],
            'cash': self.current_holdings['cash'],
            'commission': self.current_holdings['commission'],
            'total': self.current_holdings['total']
        })

    def process_signal(self, event: events.SignalEvent):
        """
        Simply transacts an OrderEvent object as a constant quantity
        sizing of the signal object, without risk management or
        position sizing considerations.
        """
        order_type = 'MKT'

        # TODO strength = signal.strength, mkt_quantity = floor(100 * strength)
        cur_quantity = self.current_position[self.symbol]
        mkt_quantity = 0

        for signal in event.signal:

            if signal == 'EXIT':
                if cur_quantity == 0:
                    continue
                mkt_quantity -= cur_quantity
            # TODO check if account balance ('cash') is sufficient to process the purchase
            amount = self.current_holdings['total'] * self.bet_size / event.last_close_price
            if signal == 'LONG':
                mkt_quantity += amount
            if signal == 'SHORT':
                mkt_quantity -= amount

        # mkt_quantity can be positive (byu) or negative (sell)
        order_event = events.OrderEvent(self.symbol, event.bar_close_time,
                                        order_type, mkt_quantity, event.last_close_price)
        self.events.put(order_event)

    def update_fill(self, event: events.FillEvent):
        """
        Updates the portfolio current positions and holdings
        from a FillEvent.
        NO SHORTS for now
        """

        self.current_position[event.symbol] += event.quantity

        self.current_holdings[event.symbol] += event.quantity
        assert self.current_holdings[event.symbol] == self.current_position[event.symbol]

        self.current_holdings['cash'] -= event.quantity * event.price_filled + event.commission

        self.current_holdings['commission'] += event.commission
        self.current_holdings['total'] = (self.current_holdings['cash'] +
                                          self.current_holdings[event.symbol] * event.price_filled)

        self.buffer.append_data({
            'close_time': event.bar_close_time,
            self.symbol: self.current_holdings[self.symbol],
            'price_filled': event.price_filled,
            'cash': self.current_holdings['cash'],
            'commission': self.current_holdings['commission'],
            'total': self.current_holdings['total']
        })

        print(
            f'bought:{round(event.quantity, 3)} {event.symbol}, '
            f'have_now:{round(self.current_holdings[event.symbol], 3)}, '
            f'total:{round(self.current_holdings["total"], 3)}, '
            f'cash:{round(self.current_holdings["cash"])}, '
            f'comm:{round(self.current_holdings["commission"])}, '
            f'price:{round(event.price_filled, 2)}'
        )




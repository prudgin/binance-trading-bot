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

    def __init__(self, events, symbol, initial_capital=100000.0):
        """
        Initialises the portfolio with bars and an event queue.
        Also includes a starting datetime index and initial capital
        (USD unless otherwise stated).

        Parameters:
        bars - The DataHandler object with current market data.
        events - The Event Queue object.
        start_date - The start date (bar) of the portfolio.
        initial_capital - The starting capital in USD.
        """
        self.symbol = symbol
        self.events = events
        self.start_date = int(time.time() * 1000)
        self.initial_capital = initial_capital

        self.current_position = {self.symbol: 0}

        # TODO how big are those lists going to grow? Need to put a limit, or write into a database
        self.all_positions = [{self.symbol: 0, 'datetime': self.start_date}]

        self.current_holdings = {
            self.symbol: 0,
            'cash': self.initial_capital,
            'commission': 0,
            'total': self.initial_capital
        }

        self.all_holdings = [{
            self.symbol: 0,
            'datetime': self.start_date,
            'cash': self.initial_capital,
            'commission': 0,
            'total': self.initial_capital
        }]

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

        dict_to_append = copy.deepcopy(self.current_holdings)
        # TODO new_data can be a list of dicts!
        dict_to_append['datetime'] = new_data['close_time']
        self.all_holdings.append(dict_to_append)


    def process_signal(self, event: events.SignalEvent):
        """
        Simply transacts an OrderEvent object as a constant quantity
        sizing of the signal object, without risk management or
        position sizing considerations.
        """
        # TODO strength = signal.strength, mkt_quantity = floor(100 * strength)
        cur_quantity = self.current_position[self.symbol]
        print(f'cur quant: {cur_quantity}')

        order_type = 'MKT'
        if event.signal_type == 'LONG':
            direction = 'BUY'
            if cur_quantity == 0:
                mkt_quantity = self.current_holdings['cash']*0.2
            elif cur_quantity < 0:
                mkt_quantity = -1 * cur_quantity
            else:
                # TODO throw an error?
                print('4ego tebe eshe nado, SOBAKA????')
                mkt_quantity = 0

        elif event.signal_type == 'SHORT':
            direction = 'SELL'
            if cur_quantity == 0:
                mkt_quantity = self.current_holdings['cash']*0.2
            elif cur_quantity > 0:
                mkt_quantity = cur_quantity
            else:
                # TODO throw an error?
                print('4ego tebe eshe nado, SOBAKA????')
                mkt_quantity = 0

        # TODO if direction == 'EXIT' and cur_quantity > 0: exit market

        order_event = events.OrderEvent(self.symbol, order_type, mkt_quantity, direction)
        self.events.put(order_event)










    def update_positions_from_fill(self, fill):
        """
        Takes a FillEvent object and updates the position matrix
        to reflect the new position.

        Parameters:
        fill - The FillEvent object to update the positions with.
        """
        # Check whether the fill is a buy or sell
        fill_dir = 0
        if fill.direction == 'BUY':
            fill_dir = 1
        if fill.direction == 'SELL':
            fill_dir = -1

        # Update positions list with new quantities
        self.current_positions[fill.symbol] += fill_dir * fill.quantity

    def update_holdings_from_fill(self, fill):
        """
        Takes a FillEvent object and updates the holdings matrix
        to reflect the holdings value.

        Parameters:
        fill - The FillEvent object to update the holdings with.
        """
        # Check whether the fill is a buy or sell
        fill_dir = 0
        if fill.direction == 'BUY':
            fill_dir = 1
        if fill.direction == 'SELL':
            fill_dir = -1

        # Update holdings list with new quantities
        fill_cost = self.bars.get_latest_bars(fill.symbol)[0][5]  # Close price
        cost = fill_dir * fill_cost * fill.quantity
        self.current_holdings[fill.symbol] += cost
        self.current_holdings['commission'] += fill.commission
        self.current_holdings['cash'] -= (cost + fill.commission)
        self.current_holdings['total'] -= (cost + fill.commission)

    def update_fill(self, event):
        """
        Updates the portfolio current positions and holdings
        from a FillEvent.
        """
        if event.type == 'FILL':
            self.update_positions_from_fill(event)
            self.update_holdings_from_fill(event)





    def create_equity_curve_dataframe(self):
        """
        Creates a pandas DataFrame from the all_holdings
        list of dictionaries.
        """
        curve = pd.DataFrame(self.all_holdings)
        curve.set_index('datetime', inplace=True)
        curve['returns'] = curve['total'].pct_change()
        curve['equity_curve'] = (1.0 + curve['returns']).cumprod()
        self.equity_curve = curve

import sys
import queue
import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt
import time
from datetime import timedelta

from historical_data import helper_functions as hlp
import data
import buffer as buffer_module
import strategy
import portfolio as portfolio_module
import execution
import performance

class BackTester():
    """
    A class to run an event driven backtester with given parameters
    """
    def __init__(self, strategy: strategy.Strategy,
                 symbol: str, interval: str,
                 initial_capital=10000, bet_size=1):
        """
        :param strategy: the strategy to be tested
        :param symbol: the symbol to test on, for example 'BTCUSDT'
        :param interval: time interval, e.g. '1h' for one hour scale or '1d' for one day
        :param start: time ofbacktest start, e.g. '01-Mar-2019 00:00:00'
        :param end: time of backtester stop, e.g. '30-Mar-2019 00:00:00'
        :param initial_capital: well, the capital to start with
        :param bet_size: what portion of the initial capital is used per each trade
        """
        self.symbol = symbol
        self.interval = interval

        self.initial_capital = initial_capital
        self.bet_size = bet_size
        self.strategy = strategy

        self.interval_ts = hlp.interval_to_milliseconds(interval)

    def run_test(self, start: str, end: str, draw=False, print_results=True, imported_data=False):
        """
        :param start:
        :param end:
        :param draw:
        :param print_results:
        :param imported_data_data: you can initialise backtester with historical data in case you have it. Otherwise data
        will be downloaded.
        :return:
        """

        self.start = start
        self.end = end
        self.start_ts = hlp.date_to_milliseconds(start)
        self.end_ts = hlp.date_to_milliseconds(end)

        events = queue.Queue()
        buffer = buffer_module.DataBuffer(self.symbol)

        data_handler = data.HistoricDataHandler(events, buffer, self.symbol, self.interval,
                                                self.start_ts, self.end_ts, delete_previous_data=False,
                                                imported_data=imported_data)


        portfolio = portfolio_module.NaivePortfolio(events, buffer, self.symbol,
                                             initial_capital=self.initial_capital,
                                             bet_size=self.bet_size, start_ts=self.start_ts)

        executor = execution.SimulatedExecutionHandler(events)

        while True:
            # Update the bars
            if data_handler.continue_backtest:
                #  get new bar from data feed, append it to buffer (queue)
                data_handler.update_bars()
            else:
                break
            # Process event queue until it's empty
            while True:
                try:
                    event = events.get(False)  # https://docs.python.org/3/library/queue.html#queue.Queue.get
                except queue.Empty:
                    break

                if event.type == 'MARKET':
                    # event fired by data_handler.update_bars(), contains new data
                    self.strategy.calculate_signals(self.symbol, events, event, buffer)
                    portfolio.update_timeindex(event)

                elif event.type == 'SIGNAL':
                    portfolio.process_signal(event)  # puts an order event on the queue

                elif event.type == 'ORDER':
                    executor.execute_order(event)

                elif event.type == 'FILL':
                    portfolio.update_fill(event, verbose=False)


        backtest_results = performance.calculate_performance(buffer, self.interval, 2)
        if backtest_results is not None:
            # print results:
            if print_results:
                print(f'mean annual return: {round(backtest_results["mean_annual_return"])}%')
                print(f'mean annual disc return: {round(backtest_results["mean_annual_disc_return"])}%')
                print(f'total return: {round(backtest_results["total_return"])}%')
                print(f'annualised total return: {round(backtest_results["annualised_total_return"])}%')
                print(f'Sharpe ratio: {round(backtest_results["sharpe_ratio"], 2)}')
                print(f'max drawdawn: {round(backtest_results["max_drawdown"])}%')
                print(f'max drawdawn duration: {backtest_results["max_drawdown_duration"]}')
                print(f'test duration: {backtest_results["test_duration"]}')
                print(f'max drawdown duration: '
                      f'{round(backtest_results["drawdown_duration_percent"]*100)}%')
                print(f'backtest run for {timedelta(milliseconds=self.end_ts - self.start_ts)}')
            # plotiing:
            if draw:
                buffer.draw(backtest_results['drawdown_df'])

        return backtest_results

import sys
import queue
import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt
import time
from datetime import timedelta

from historical_data import helper_functions as hlp
import data
import buffer
import strategy
import portfolio
import execution
import performance

events = queue.Queue()

symbol = 'ADAUSDT'
start_ts = hlp.date_to_milliseconds('01-Mar-2019 00:00:00')
end_ts = hlp.date_to_milliseconds('30-Mar-2019 00:00:00')
interval = '1h'
interval_ts = hlp.interval_to_milliseconds(interval)

buffer = buffer.DataBuffer(symbol)
data_handler = data.HistoricDataHandler(events, buffer, symbol, interval,
                                        start_ts, end_ts, delete_previous_data=False)

ema_strategy = strategy.EMAStrategy(10, 50)

portfolio = portfolio.NaivePortfolio(events, buffer, symbol, initial_capital=10000, bet_size=1, start_ts=start_ts)

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
            ema_strategy.calculate_signals(symbol, interval_ts, events, event, buffer)
            portfolio.update_timeindex(event)

        elif event.type == 'SIGNAL':

            portfolio.process_signal(event)  # puts an order event on the queue

        elif event.type == 'ORDER':

            executor.execute_order(event)

        elif event.type == 'FILL':
            portfolio.update_fill(event)


backtest_results = performance.calculate_performance(buffer, interval, 2)

# print results:
print(f'mean annual return: {round(backtest_results["mean_annual_return"])}%')
print(f'mean annual disc return: {round(backtest_results["mean_annual_disc_return"])}%')
print(f'total return: {round(backtest_results["total_return"])}%')
print(f'Sharpe ratio: {round(backtest_results["sharpe_ratio"],2)}')
print(f'max drawdawn: {round(backtest_results["max_drawdown"])}%')
print(f'max drawdawn duration: {backtest_results["max_drawdown_duration"]}')
print(f'backtest run for {timedelta(milliseconds=end_ts - start_ts)}')

# plotiing:
buffer.draw(backtest_results['drawdown_df'])

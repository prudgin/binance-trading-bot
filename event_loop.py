import sys
import queue
import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt
import time

from historical_data import helper_functions as hlp
import data
import buffer
import strategy
import portfolio
import execution


events = queue.Queue()

symbol = 'BTCUSDT'
start_ts = hlp.date_to_milliseconds('01-Dec-2018 00:00:00')
end_ts = hlp.date_to_milliseconds('01-Jan-2020 00:00:00')
interval = '1d'
interval_ts = hlp.interval_to_milliseconds(interval)

buffer = buffer.DataBuffer(symbol)
data_handler = data.HistoricDataHandler(events, buffer, symbol, interval,
                                        start_ts, end_ts, delete_previous_data=False)

ema_strategy = strategy.EMAStrategy(events, buffer, symbol, interval_ts, 10, 50)

portfolio = portfolio.NaivePortfolio(events, buffer, symbol, initial_capital=10000, start_ts=start_ts)

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
            ema_strategy.calculate_signals(event)  # event fired by data_handler.update_bars(), contains new data
            portfolio.update_timeindex(event)

        elif event.type == 'SIGNAL':

            portfolio.process_signal(event)  # puts an order event on the queue

        elif event.type == 'ORDER':

            executor.execute_order(event)

        elif event.type == 'FILL':
            portfolio.update_fill(event)


buffer.draw()



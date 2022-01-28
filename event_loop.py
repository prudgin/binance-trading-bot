import queue
import talib
import numpy as np
import pandas as pd
import mplfinance as mpf

import helper_functions as hlp
import data
import events
import strategy
import portfolio


def plot_buffers(data_handler, ema_strategy):

    prepared = hlp.prepare_df_for_plotting(pd.DataFrame(data_handler.buffered_data))

    plot_df = pd.DataFrame(ema_strategy.buffer)[['open_time',
                                                 ema_strategy.ema_fast.name,
                                                 ema_strategy.ema_slow.name]].copy(deep=True)
    plot_df['open_time'] = pd.to_datetime(plot_df['open_time'], unit='ms')
    plot_df = plot_df.set_index('open_time')
    plot_df = plot_df.astype('float')
    ap = mpf.make_addplot(plot_df[[ema_strategy.ema_fast.name,
                                   ema_strategy.ema_slow.name]])
    mpf.plot(prepared, type='candle', addplot=ap)


events = queue.Queue()

symbol = 'BTCUSDT'

ema_strategy = strategy.EMAStrategy(events, symbol, 2, 30, max_buffer_size=50)

data_handler = data.HistoricDataHandler(events, symbol, '1h', start_ts=1543104000000,
                                        end_ts=1543104000000 - 1 + 60000 * 60 * 200,
                                        max_buffer_size=50)

portfolio = portfolio.NaivePortfolio(events, symbol, initial_capital=10000)
# broker = ExecutionHandler(...)


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
            ema_strategy.calculate_signals(event) # event fired by data_handler.update_bars(), contains new data
            portfolio.update_timeindex(event)

        elif event.type == 'SIGNAL':
            plot_buffers(data_handler, ema_strategy)
            portfolio.process_signal(event)  # puts an order event on the queue

        elif event.type == 'ORDER':

            event.print_order()
            pass
            # broker.execute_ordder(event)

        elif event.type == 'FILL':
            pass
            # portfolio.update_fill(event)

import queue
import talib
import numpy as np
import pandas as pd
import mplfinance as mpf

import helper_functions as hlp
import data
import events
import strategy


events = queue.Queue()

ema_strategy = strategy.EMAStrategy(events, 5, max_buffer_size=20)

data_handler = data.HistoricDataHandler(events, 'BTCUSDT', '1h', start_ts=1543104000000,
                                end_ts=1543104000000 - 1 + 60000 * 60 * 20,
                                max_buffer_size=20)


# portfolio = Portfolio(...)
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
            ema_strategy.calculate_signals(data_handler.buffered_data[-1])

            if len(ema_strategy.buffer) > 4:
                feed_data = list(data_handler.buffered_data)

                # ve have a list of dicts now

                tal_ema = talib.EMA(np.asarray([float(i['close']) for i in feed_data]), timeperiod=ema_strategy.n)

                tal_ema = pd.Series(np.nan_to_num(tal_ema))

                plot_df = pd.DataFrame(ema_strategy.buffer)[['open_time', 'ema']].copy(deep=True)
                plot_df['open_time'] = pd.to_datetime(plot_df['open_time'], unit='ms')
                plot_df['talib'] = tal_ema
                plot_df = plot_df.set_index('open_time')
                plot_df = plot_df.astype('float')
                plot_df = plot_df[4:]

                ap = mpf.make_addplot(plot_df[['ema', 'talib']])

                prepared = hlp.prepare_df_for_plotting(pd.DataFrame(feed_data))[4:]
                mpf.plot(prepared, type='candle', addplot=ap)

                print(ema_strategy.buffer[-1]['ema'])
                print(list(tal_ema)[-1])

            #portfolio.update_timeindex(event)  # Part V

        elif event.type == 'SIGNAL':
            pass
            # portfolio.update_signal(event)  # Part V

        elif event.type == 'ORDER':
            pass
            # broker.execute_ordder(event)

        elif event.type == 'FILL':
            pass
            # portfolio.update_fill(event)

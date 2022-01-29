import sys
import queue
import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt
import time

from historical_data import helper_functions as hlp
import data
import strategy
import portfolio
import execution

events = queue.Queue()
symbol = 'DOTUSDT'
start_ts = hlp.date_to_milliseconds('17-Aug-2020 00:00:00')
end_ts = end_ts = int(time.time() * 1000)
ema_strategy = strategy.EMAStrategy(events, symbol, 10, 30, max_buffer_size=50)
data_handler = data.HistoricDataHandler(events, symbol, '1d', start_ts, end_ts,
                                        max_buffer_size=50, delete_previous_data=False)
portfolio = portfolio.NaivePortfolio(events, symbol, initial_capital=10000, start_ts=start_ts)
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

            str1 = "bought:{0} {1}, have_now:{2}, total:{3}, cash:{4}, comm:{5}, price:{6}".format(
                round(event.quantity, 3),
                event.symbol,
                round(portfolio.current_holdings[event.symbol], 3),
                round(portfolio.current_holdings['total']),
                round(portfolio.current_holdings['cash']),
                round(portfolio.current_holdings['commission']),
                round(event.price_filled, 2)
            )
            if event.quantity > 0:
                str1 = str1[:7] + ' ' + str1[7:31] + ' ' + str1[31:]
            print(str1)


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


# plot_buffers(data_handler, ema_strategy)

portfolio_df = pd.DataFrame(portfolio.all_holdings)[['datetime', 'total']]
portfolio_df['datetime'] = pd.to_datetime(portfolio_df['datetime'], unit='ms')
portfolio_df = portfolio_df.set_index('datetime')



signals_df = pd.DataFrame(portfolio.all_positions[1:])[['datetime', portfolio.symbol]]
signals_df['datetime'] = pd.to_datetime(signals_df['datetime'], unit='ms')
signals_df['datetime2'] = signals_df['datetime'].copy()
signals_df = signals_df.set_index('datetime')
signals_df['color'] = ['green' if x > 0 else 'red' for x in signals_df[portfolio.symbol]]



hist_prices = [(i[4], i[6]) for i in data.HistoricDataHandler(events, symbol, '1d', start_ts, end_ts,
                                                      max_buffer_size=50, delete_previous_data=False).historical_data]
prices_df = pd.DataFrame(hist_prices, columns=['close_price', 'datetime'])
prices_df['datetime'] = pd.to_datetime(prices_df['datetime'], unit='ms')
prices_df = prices_df.set_index('datetime')

signals_df = pd.concat([signals_df[['color','datetime2']], prices_df], axis=1, join='inner')

ax1 = portfolio_df.plot(color = 'orange')
ax2 = ax1.twinx()

ax2.spines['right'].set_position(('axes', 1.0))

prices_df.plot(ax=ax2)

signals_df.plot.scatter(x='datetime2', y='close_price', c='color' , ax=ax2)

plt.show()
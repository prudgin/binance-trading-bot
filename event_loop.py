import queue
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
            ema_strategy.calculate_signals(data_handler.buffered_data)
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

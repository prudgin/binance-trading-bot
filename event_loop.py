import queue
import data
import events

events = queue.Queue()

bars = data.HistoricDataHandler(events, 'BTCUSDT', '1d', start_ts=1543104000000,
                                end_ts=1543104000000 - 1 + 60000 * 60 * 100)

# strategy = Strategy(...)
# portfolio = Portfolio(...)
# broker = ExecutionHandler(...)


while True:
    # Update the bars
    if bars.continue_backtest:
        bars.update_bars()
    else:
        break
    # Process event queue until it's empty
    while True:
        try:
            event = events.get(False)  # https://docs.python.org/3/library/queue.html#queue.Queue.get
        except queue.Empty:
            break

        if event.type == 'MARKET':
            pass
            # strategy.calculate_signals(event)
            # portfolio.update_timeindex(event)  # Part V

        elif event.type == 'SIGNAL':
            pass
            # portfolio.update_signal(event)  # Part V

        elif event.type == 'ORDER':
            pass
            # broker.execute_ordder(event)

        elif event.type == 'FILL':
            pass
            # portfolio.update_fill(event)

import strategy
import backtesting

import historical_data.helper_functions as hlp

symbol = 'SOLUSDT'
interval = '1d'
interval_ts = hlp.interval_to_milliseconds(interval)
start = '01-Mar-2019 00:00:00'
end = '30-Mar-2021 00:00:00'


ema_strategy = strategy.EMAStrategy(interval_ts, 10, 50)

backtester = backtesting.BackTester(ema_strategy, symbol, interval, start, end)

backtester.run_test(draw=False, print_results=True)

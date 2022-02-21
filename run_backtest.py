import pandas as pd
import strategy
import backtesting
import optimization

import btb_helpers as hlp

symbol = 'BTCUSDT'
interval = '1d'
start = '01-Jan-2017 00:00:00'
end = '30-Mar-2022 00:00:00'

interval_ts = hlp.interval_to_milliseconds(interval)

ema_strategy = strategy.EMAStrategy(interval_ts, 10, 50)

backtester = backtesting.BackTester(ema_strategy, symbol, interval)

backtest_results = backtester.run_test(start, end, draw=True, print_results=True)


#print(pd.DataFrame(backtest_results).columns)

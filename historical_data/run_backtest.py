import strategy
import backtesting
import optimization

import historical_data.helper_functions as hlp

symbol = 'SOLUSDT'
interval = '1d'
start = '01-Mar-2019 00:00:00'
end = '30-Mar-2022 00:00:00'

interval_ts = hlp.interval_to_milliseconds(interval)


ema_strategy = strategy.EMAStrategy(interval_ts, 50, 234)

backtester = backtesting.BackTester(ema_strategy, symbol, interval)

backtest_results = backtester.run_test(start, end, draw=True, print_results=True)


af_score = (
    backtest_results['annualised_total_return'] * 1 +
    backtest_results['sharpe_ratio'] * 100 * 1 +
    backtest_results['max_drawdown'] * 1 -
    backtest_results['drawdown_duration_percent'] * 1
)

print(f'af_score: {str(int(af_score))}')

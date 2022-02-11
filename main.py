import strategy
import backtesting
import optimization

import historical_data.helper_functions as hlp

symbol = 'SOLUSDT'
interval = '1d'
start = '01-Mar-2019 00:00:00'
end = '30-Mar-2022 00:00:00'


ema_strategy = strategy.EMAStrategy

optimizer = optimization.Optimizer(symbol, interval)

param_ranges = {'fast': (10, 150), 'slow': (20, 250)}

optimizer.optimize_ema(ema_strategy, start, end, param_ranges, n_points=15)


# TODO
# backtesting and optimization: 1 event_loop = 1 strategy = 1 backtester = 1 symbol
# live trading: shared event loop for N strategies. Each strategy has it's own portfolio, buffer, backtester.
# Each backtester process should tend to start on a new core
# mb order executors should be coroutines

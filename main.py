import pandas as pd
import numpy as np

from sklearn.cluster import DBSCAN
from sklearn import metrics
from sklearn.preprocessing import StandardScaler
from sklearn.manifold import TSNE

import strategy
import backtesting
import optimization




symbol = 'ETHUSDT'
interval = '1d'
start = '01-Jan-2017 00:00:00'
end = '30-Mar-2022 00:00:00'


ema_strategy = strategy.EMAStrategy

optimizer = optimization.Optimizer(symbol, interval)

param_ranges = {'fast': (1, 100), 'slow': (10, 250)}

optimizer_results = optimizer.optimize_ema(ema_strategy, start, end, param_ranges, n_points=20)
optimizer_results = pd.DataFrame(optimizer_results)

optimizer_results.to_csv('opt.csv', index=False)



# TODO
# backtesting and optimization: 1 event_loop = 1 strategy = 1 backtester = 1 symbol
# live trading: shared event loop for N strategies. Each strategy has it's own portfolio, buffer, backtester.
# Each backtester process should tend to start on a new core
# mb order executors should be coroutines

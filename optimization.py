from itertools import product
import copy
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

import strategy
import backtesting

from historical_data import get_hist_data as ghd
import historical_data.helper_functions as hlp


class Optimizer():
    def __init__(self, symbol, interval):
        self.symbol = symbol
        self.interval = interval
        self.interval_ts = hlp.interval_to_milliseconds(interval)

    def optimize_ema(self, strategy, start: str, end: str, param_ranges: dict, n_points=5):
        self.start = start
        self.end = end
        self.start_ts = hlp.date_to_milliseconds(start)
        self.end_ts = hlp.date_to_milliseconds(end)

        self.candlestick_data = ghd.get_candles_from_db(self.symbol, self.interval, self.start_ts, self.end_ts,
                                                        delete_existing_table=False)

        # transforms a param_ranges dict like {'fast' : (5, 50), 'slow' : (10, 200)} into a dict like:
        # {'fast' : [5, 16, 28, 39, 50], 'slow' : [10, 58, 105, 152, 200]}
        # number of elements in a list is equal to n_points
        param_ranges = {k: hlp.span_to_list(v, n_points) for k, v in param_ranges.items()}
        print(f'going to optimize in parameter ranges: {param_ranges}')

        # create a list of test points, like [{'fast' : 5, 'slow' : 10}, {'fast' : 5, 'slow' : 58}, ...]
        kwargs_points = [
            dict(zip(param_ranges, cortesian_product_item))
            for cortesian_product_item
            in product(*param_ranges.values())
        ]
        # includes slow < fast
        # TODO filter slow = fast or very close
        kwargs_points = [d for d in kwargs_points if d['fast'] < d['slow'] - 5]

        optimization_results = []

        for kwargs in kwargs_points:

            self.strategy = strategy(self.interval_ts, **kwargs)

            self.backtester = backtesting.BackTester(self.strategy, self.symbol, self.interval)

            # need to copy the data, because data handler uses data.pop, and data gets deplenished after first run
            backtest_results = self.backtester.run_test(self.start, self.end, draw=False, print_results=False,
                                                        imported_data=copy.deepcopy(self.candlestick_data))

            if backtest_results:
                af_score = (
                        backtest_results['annualised_total_return'] * 1 +
                        backtest_results['sharpe_ratio'] * 100 * 1 +
                        backtest_results['max_drawdown'] * 1 -
                        backtest_results['drawdown_duration_percent'] * 1
                )
                print(f'af_score: {str(int(af_score))}, fast: {kwargs["fast"]}, slow: {kwargs["slow"]}')

                results_of_interest = [
                    'mean_annual_return',
                    'annualised_total_return',
                    'sharpe_ratio',
                    'max_drawdown',
                    'drawdown_duration_percent'
                ]
                results_of_interest = [i for i in results_of_interest if i in backtest_results]
                result = {key: backtest_results[key] for key in results_of_interest}
                result['af_score'] = af_score
                result = {**result, **kwargs}
                optimization_results.append(result)


        df_af_score = pd.DataFrame(optimization_results).pivot('fast', 'slow', 'af_score')
        df_sharpe = pd.DataFrame(optimization_results).pivot('fast', 'slow', 'sharpe_ratio')
        df_return = pd.DataFrame(optimization_results).pivot('fast', 'slow', 'annualised_total_return')
        df_max_drawdown = pd.DataFrame(optimization_results).pivot('fast', 'slow', 'max_drawdown')


        fig, ax = plt.subplots(nrows=2, ncols=2)
        sns.heatmap(df_af_score, annot=False, fmt=".1f", ax = ax[0, 0])
        sns.heatmap(df_return, annot=False, fmt=".1f", ax=ax[1, 0])
        sns.heatmap(df_sharpe, annot=True, fmt=".1f", ax=ax[0, 1])
        sns.heatmap(df_max_drawdown, annot=False, fmt=".1f", ax=ax[1, 1])
        ax[0, 0].title.set_text('af_score')
        ax[1, 0].title.set_text('return')
        ax[0, 1].title.set_text('sharpe')
        ax[1, 1].title.set_text('max_drawdown')

        plt.show()

        return optimization_results

# TODO
# backtesting and optimization: 1 event_loop = 1 strategy = 1 backtester = 1 symbol
# live trading: shared event loop for N strategies. Each strategy has it's own portfolio, buffer, backtester.
# Each backtester process should tend to start on a new core
# mb order executors should be coroutines

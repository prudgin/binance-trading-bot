import buffer
import numpy as np
import pandas as pd
import logging

import btb_helpers as hlp
import mplfinance as mpf
import matplotlib.pyplot as plt
from datetime import timedelta


logger = logging.getLogger(__name__)


def calculate_performance(buffer: buffer.DataBuffer, interval: str, initial_capital: int,
                          riskless_ann_return=0, draw=False, print_results=False) -> dict:
    """
    :param buffer:
    :param interval:
    :param riskless_ann_return: percent
    :return: mean_annual_return, mean_annual_disc_return, total_return,
     sharpe_ratio, max_drawdown, max_drawdown_duration, drawdown_df]
    """

    buffer_data = buffer.get_all_data()
    if buffer_data is None:
        print(f'data seem to be empty, no trading occured')
        return None

    if 'price_filled' not in buffer_data.columns:
        print('seems like no bargains were made, performance returning None')
        return None

    # Time of first and last trade
    first_trade = buffer_data['price_filled'].first_valid_index()
    last_trade = buffer_data['price_filled'].last_valid_index()
    if not first_trade and last_trade:
        print('less then 2 trades were made, performance returning None')
        return None

    # Close trade parameters (page 30):
    # It is the difference between the entry and the exit price, without taking into consideration
    # what is going on within the trade.
    # End trade parameters, e.g. drawdown, tells us how much of the open profit we had to give back
    # before we were allowed to exit a specific trade.
    # I will calculate close trade parameters here

    # take param names from buffer (for example 'fast' and 'slow' in case of 2 ema crossover)
    param_names = buffer.get_params()
    symbol = buffer.get_symbol()
    if not symbol and param_names:
        logger.error('performance.py: buffer seems to be not initialised')
        return None

    cols_list = ['total', 'open', 'high', 'low', 'close', 'volume',
                 'price_filled', 'close_time', symbol, *param_names]

    if not all([True if x in buffer_data.columns else False for x in cols_list]):
        logger.error(f'performance.py: buffer misses some essential columns:'
                     f' {[x for x in cols_list if x not in buffer_data.columns]}')
        return None

    df = buffer_data[cols_list].copy()

    # total is total balance
    # price_filled is not NaN only if a trade was made
    df['total_filled'] = df[df.price_filled.notna()]['total']
    df['total_filled'].fillna(method='ffill', inplace=True)
    # Fill total preceding the first trade with initial capital
    df['total_filled'].fillna(initial_capital, inplace=True)

    # Calculate number of candles in a year, need to calculate Sharpe ratio
    interval_ts = hlp.interval_to_milliseconds(interval)
    year = 86400 * 365 * 1000  # in ms
    periods = year / interval_ts

    df['total_prev'] = df['total_filled'].shift(1)
    df['returns'] = (df['total_filled'] - df['total_prev']) / df['total_prev']
    df['disc_returns'] = df['returns'] - (riskless_ann_return / (100 * periods))

    sharpe_ratio = np.sqrt(periods) * np.mean(df['disc_returns']) / np.std(df['disc_returns'])
    mean_annual_return = 100 * periods * np.mean(df["returns"])
    mean_annual_disc_return = 100 * periods * np.mean(df["disc_returns"])
    total_return = 100 * (df.total_filled[-1] - df.total_filled[0]) / df.total_filled[0]

    water_mark = df['total_filled'][0]
    wms = []
    max_duration = timedelta(0)
    last_peak_time = df.index[0]
    for i in df.index:
        cur_total = df['total_filled'][i]
        if cur_total >= water_mark:
            wms.append(cur_total)
            water_mark = cur_total
            max_duration = max(max_duration, i - last_peak_time)
            last_peak_time = i
        else:
            wms.append(water_mark)
    max_duration = max(max_duration, df.index[-1] - last_peak_time)

    df['watermark'] = wms
    df['underwater'] = 100 * (df['total_filled'] - df['watermark']) / df['watermark']
    max_drawdown = df["underwater"].min()
    max_drawdown_duration = max_duration
    test_duration = df.index[-1] - df.index[0]
    # Time passed betwen first and last trade
    first_last_duration = last_trade - first_trade
    annualised_total_return = 365 * total_return / first_last_duration.days
    drawdown_duration_percent = max_drawdown_duration / first_last_duration

    results = {
        'mean_annual_return': mean_annual_return,
        'mean_annual_disc_return': mean_annual_disc_return,
        'total_return': total_return,
        'annualised_total_return': annualised_total_return,
        'sharpe_ratio': sharpe_ratio,
        'max_drawdown': max_drawdown,
        'max_drawdown_duration': max_drawdown_duration,
        'test_duration': test_duration,
        'first_last_duration': first_last_duration,
        'drawdown_duration_percent': drawdown_duration_percent,
        'drawdown_df': df['underwater']
    }


####################################################################################################
# PLOT DRAWING HERE
####################################################################################################

    if draw:

        apds = [mpf.make_addplot(df[param_names])]

        if 'price_filled' in df.columns:
            df['up'] = df['price_filled'].loc[df[symbol] > 0]
            df['down'] = df['price_filled'].loc[df[symbol] < 0]

            if df['up'].notna().any():
                apds.append(mpf.make_addplot(df['up'], type='scatter', markersize=100, marker='^', color='green'))
            if df['down'].notna().any():
                apds.append(mpf.make_addplot(df['down'], type='scatter', markersize=100, marker='v', color='red'))
            if df['total_filled'].notna().any():
                apds.append(mpf.make_addplot(df['total_filled'],
                                             secondary_y=True, color='brown', ylabel='balance'))


        apds.append(mpf.make_addplot(df['underwater'], panel=2, ylabel='drawdown %'))

        fig, axlist = mpf.plot(
            df[['open', 'high', 'low', 'close', 'volume']],
            type='candle',
            volume=True,
            addplot=apds,
            returnfig=True
        )
        axlist[1].legend(['balance total'])

        plt.show()

####################################################################################################
# PRINT RESULTS
####################################################################################################

    if print_results:
        print(f'mean annual return: {round(results["mean_annual_return"])}%')
        print(f'mean annual disc return: {round(results["mean_annual_disc_return"])}%')
        print(f'total return: {round(results["total_return"])}%')
        print(f'annualised total return: {round(results["annualised_total_return"])}%')
        print(f'Sharpe ratio: {round(results["sharpe_ratio"], 2)}')
        print(f'max drawdawn: {round(results["max_drawdown"])}%')
        print(f'max drawdawn duration: {results["max_drawdown_duration"]}')
        print(f'test duration: {results["test_duration"]}')
        print(f'time btwen first and last trade: {results["first_last_duration"]}')
        print(f'max drawdown duration: '
              f'{round(results["drawdown_duration_percent"] * 100)}%')





    return

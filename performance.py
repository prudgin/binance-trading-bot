import buffer
import numpy as np
import pandas as pd
import btb_helpers as hlp
import mplfinance as mpf
import matplotlib.pyplot as plt
from datetime import timedelta


def calculate_performance(buffer: buffer.DataBuffer, interval: str, riskless_ann_return=0, draw=False) -> dict:
    """
    :param buffer:
    :param interval:
    :param riskless_ann_return: percent
    :return: mean_annual_return, mean_annual_disc_return, total_return,
     sharpe_ratio, max_drawdown, max_drawdown_duration, drawdown_df]
    """


    interval_ts = hlp.interval_to_milliseconds(interval)

    year = 86400 * 365 * 1000  # in ms
    periods = year / interval_ts

    buffer_data = buffer.get_all_data()
    if buffer_data is None:
        print(f'data seem to be empty, no trading occured')
        return None

    if 'price_filled' not in buffer_data.columns:
        print('seems like no bargains were made, performance returning None')
        return None


    # Close trade parameters (page 30):
    # It is the difference between the entry and the exit price, without taking into consideration
    # what is going on within the trade. I will call them fill_parameter
    # End trade parameters, e.g. drawdown, tells us how much of the open profit we had to give back
    # before we were allowed to exit a specific trade.

    #df_fill = df[df.price_filled.notna()]['total']
    #print(df_fill)


    df = buffer_data[['close_time', 'total']].copy()
    #df = buffer_data[buffer_data.price_filled.notna()][['close_time', 'total']].copy()


    df['total_prev'] = df['total'].shift(1)
    df['returns'] = (df['total'] - df['total_prev']) / df['total_prev']
    df['disc_returns'] = df['returns'] - (riskless_ann_return / (100 * periods))

    sharpe_ratio = np.sqrt(periods) * np.mean(df['disc_returns']) / np.std(df['disc_returns'])
    mean_annual_return = 100 * periods * np.mean(df["returns"])
    mean_annual_disc_return = 100 * periods * np.mean(df["disc_returns"])
    total_return = 100 * (df.total[-1] - df.total[0]) / df.total[0]

    water_mark = df['total'][0]
    wms = []
    max_duration = timedelta(0)
    last_peak_time = df.index[0]
    for i in df.index:
        cur_total = df['total'][i]
        if cur_total >= water_mark:
            wms.append(cur_total)
            water_mark = cur_total
            max_duration = max(max_duration, i - last_peak_time)
            last_peak_time = i
        else:
            wms.append(water_mark)
    max_duration = max(max_duration, df.index[-1] - last_peak_time)

    df['watermark'] = wms
    df['underwater'] = 100 * (df['total'] - df['watermark']) / df['watermark']
    max_drawdown = df["underwater"].min()
    max_drawdown_duration = max_duration
    test_duration = df.index[-1] - df.index[0]
    annualised_total_return = 365 * total_return / test_duration.days
    drawdown_duration_percent = max_drawdown_duration / test_duration

####################################################################################################
# PLOT DRAWING HERE
####################################################################################################

    if draw:

        # take param names from buffer (for example 'fast' and 'slow' in case of 2 ema crossover)
        param_names = buffer.get_params()
        symbol = buffer.get_symbol()

        cols_list = ['total', 'open', 'high', 'low', 'close', 'volume',
                     'price_filled', 'close_time', symbol, *param_names]

        cols_list = [x for x in cols_list if x in buffer_data.columns]
        df_draw = buffer_data[cols_list].copy()

        apds = [mpf.make_addplot(df_draw[param_names])]


        if 'price_filled' in df_draw.columns:
            df_draw['up'] = df_draw['price_filled'].loc[df_draw[symbol] > 0]
            df_draw['down'] = df_draw['price_filled'].loc[df_draw[symbol] < 0]

            if df_draw['up'].notna().any():
                apds.append(mpf.make_addplot(df_draw['up'], type='scatter', markersize=100, marker='^', color='green'))
            if df_draw['down'].notna().any():
                apds.append(mpf.make_addplot(df_draw['down'], type='scatter', markersize=100, marker='v', color='red'))
            if df_draw['total'].notna().any():
                apds.append(mpf.make_addplot(df_draw['total'], secondary_y=True, color='brown', ylabel='balance'))


        apds.append(mpf.make_addplot(df['underwater'], panel=2, ylabel='drawdown %'))

        fig, axlist = mpf.plot(
            df_draw[['open', 'high', 'low', 'close', 'volume']],
            type='candle',
            volume=True,
            addplot=apds,
            returnfig=True
        )
        axlist[1].legend(['balance total'])

        plt.show()




    return {
        'mean_annual_return': mean_annual_return,
        'mean_annual_disc_return': mean_annual_disc_return,
        'total_return': total_return,
        'annualised_total_return': annualised_total_return,
        'sharpe_ratio': sharpe_ratio,
        'max_drawdown': max_drawdown,
        'max_drawdown_duration': max_drawdown_duration,
        'test_duration': test_duration,
        'drawdown_duration_percent': drawdown_duration_percent,
        'drawdown_df': df['underwater']
    }

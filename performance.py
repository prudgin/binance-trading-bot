import buffer
import numpy as np
import pandas as pd
import historical_data.helper_functions as hlp
import mplfinance as mpf
import matplotlib.pyplot as plt
from datetime import timedelta


def calculate_performance(buffer: buffer.DataBuffer, interval: str, riskless_ann_return: float) -> dict:
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

    df = buffer.get_all_data()
    if df is None:
        print(f'data seem to be empty, no trading occured')
        return None

    df = df[['close_time', 'total']]
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
    drawdown_duration_percent = max_drawdown_duration/test_duration

    return {
        'mean_annual_return': mean_annual_return,
        'mean_annual_disc_return': mean_annual_disc_return,
        'total_return': total_return,
        'sharpe_ratio': sharpe_ratio,
        'max_drawdown': max_drawdown,
        'max_drawdown_duration': max_drawdown_duration,
        'test_duration': test_duration,
        'drawdown_duration_percent': drawdown_duration_percent,
        'drawdown_df': df['underwater']
    }

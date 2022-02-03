import buffer
import numpy as np
import pandas as pd
import historical_data.helper_functions as hlp


def create_sharpe_ratio(buffer: buffer.DataBuffer, interval:str, riskless_ann_return:float):
    """
    :param buffer:
    :param interval:
    :param riskless_ann_return: percent
    :return:
    """

    interval_ts = hlp.interval_to_milliseconds(interval)

    year = 86400*365*1000 #  in ms
    periods = year / interval_ts

    df = buffer.get_all_data()[['close_time', 'total']]
    df['total_prev'] = df['total'].shift(1)
    df['returns'] = (df['total'] - df['total_prev'])/df['total_prev']
    df['disc_returns'] = df['returns'] - (riskless_ann_return/(100*periods))

    sharpe_ratio = np.sqrt(periods) * np.mean(df['disc_returns']) / np.std(df['disc_returns'])

    print(f'mean annual return = {round(100 * periods * np.mean(df["returns"]))}%')
    print(f'mean annual disc return = {round(100 * periods * np.mean(df["disc_returns"]))}%')
    print(f'Sharpe ratio: {round(sharpe_ratio,2)}')



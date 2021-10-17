import logging

import pandas as pd
from binance.client import Client, AsyncClient
from binance import exceptions as pybin_exceptions
from db_interact import ConnectionDB
import time
from datetime import datetime
import exceptions
import asyncio
import spooky
from helper_functions import interval_to_milliseconds, ts_to_date

logger = logging.getLogger(__name__)





def get_candles_from_db(symbol: str, interval: str, start_ts, end_ts, limit=500, max_coroutines=50):
    # table names for each symbol and interval are created this way:
    table_name = f'{symbol}{interval}Hist'

    interval_ms = interval_to_milliseconds(interval)
    if interval_ms is None:
        logger.error('get_candles_ms got invalid interval,expected '
                     '1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w')
        return None

    if end_ts - start_ts < interval_ms:
        logger.warning('interval between requested start an end dates < chart interval')
        print('interval between requested start an end dates < chart interval, abort')
        return None

    # connecting to database
    conn_db = ConnectionDB(host=spooky.creds['host'],
                           user=spooky.creds['user'],
                           password=spooky.creds['password'],
                           database=spooky.creds['database'])
    try:
        conn_db.connect()
    except exceptions.SQLError:
        return None




    #check if table present, else create
    if not conn_db.table_in_db(table_name):
        logger.debug(f'not found table {table_name}, creating one')
        conn_db.table_create(table_name)

    # get when last entry was added to the table
    last_entry_id = conn_db.get_latest_id(table_name)

    print(f'going to fetch candles from {start_ts} {ts_to_date(start_ts)} to '
          f'{ts_to_date(end_ts)} {end_ts}')

    # check if requested data is present in database
    # search for gaps in interval from start_ta to end_ts, gap = absence of data in database
    # gaps is a list of tuples [(gap1_start, gap1_end), (gap2_start, gap2_end), ...]
    gaps = get_gaps(conn_db, table_name, interval_ms, start_ts, end_ts)
    if gaps: #if there are gaps in period of interest
        print(' found gaps:')
        for gap in gaps:
            print(f' {gap[0]} {ts_to_date(gap[0])} - {ts_to_date(gap[1])} {gap[1]}')
        for gap in gaps:
            print(f'  loading gap from exchange: {gap[0]} {ts_to_date(gap[0])} - {ts_to_date(gap[1])} {gap[1]}')
            # get candles covering gap from exchange and write them in database
            logger.debug('start asyncio.run(get_write_candles)')

            asyncio.run(get_write_candles(conn_db, symbol, table_name, interval, gap[0], gap[1]))

    logger.debug('start conn_db.get_start_end_later_than()')
    _, _, count_written = conn_db.get_start_end_later_than(table_name, last_entry_id, only_count=True)
    print(f'wrote {count_written} candles to db')


    # ok, we tried to get data, now just fetch from database:
    logger.debug('reading from db: conn_db.read()')
    #fetch = conn_db.read(table_name, start_ts, end_ts)
    conn_db.close_connection()
    #return fetch
    return 0



async def get_write_candles(conn_db, symbol, table_name, interval: str, start_ts, end_ts,
                            limit=500, max_coroutines=50):
    logger.debug('start get_write_candles')
    interval_ms = interval_to_milliseconds(interval)
    if interval_ms is None:
        logger.error('get_candles_ms got invalid interval,expected '
                     '1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w')
        return None

    # get when last entry was added to the table
    perf_start = time.perf_counter()
    logger.debug('start get_latest_id')
    last_entry_id = conn_db.get_latest_id(table_name)
    logger.debug(f'it took {time.perf_counter() - perf_start}')


    # create the Binance client, no need for api key
    start_cli = time.perf_counter()
    logger.debug('creating async client')
    exchange_client = await AsyncClient.create()
    logger.debug(f'it took {time.perf_counter() - start_cli}')

    intervals = get_limit_intervals(start_ts, end_ts, interval_ms, limit)
    resh_intervals = reshape_list(intervals, max_coroutines)

    candles_total = 0

    for bunch in resh_intervals:
        tasks = []
        for limit_interval in bunch:
            tasks.append(
                write_candles(limit_interval[0], limit_interval[1], exchange_client,
                              symbol, interval, interval_ms, limit, conn_db, table_name)
            )
        api_weight = exchange_client.response.headers['x-mbx-used-weight-1m']

        if int(api_weight) > 600:
            sleep_time = int(10 * int(api_weight) ** 3 / 1200 ** 3)
            logger.warning(f'reaching high api load, current api_weight:'
                           f' {api_weight}, max = 1200, sleep for {sleep_time} sec')
            time.sleep(sleep_time)
        logger.debug('get_write_candles: start await asyncio.gather(*tasks)')
        write_return = await asyncio.gather(*tasks)

        candles_list = [i[0] for i in write_return]
        rounded_list = [i[1] for i in write_return]
        last_in_bunch = write_return[-1][2]
        candles_sum = sum(candles_list)
        candles_total += candles_sum
        print(f'   loaded {len(candles_list)} bunches of candles, total candles: {candles_sum}, \n'
              f'    number of candles per bunch: {candles_list}')
        print(f'    rounded {rounded_list}')
        print(f'    last loaded candle: {last_in_bunch} {ts_to_date(last_in_bunch)}')

    print('  gap summary:')
    print(f'   loaded {candles_total} candles')
    logger.debug('get_write_candles: start conn_db.get_start_end_later_than')
    first_written, last_written, count_written = conn_db.get_start_end_later_than(table_name, last_entry_id)
    if first_written and last_written:
        print(f'   wrote to db {count_written} candles starting from {first_written} {ts_to_date(first_written)} to '
              f'{ts_to_date(last_written)} {last_written}')
    else:
        print(f'   wrote {count_written} candles')

    await exchange_client.close_connection()
    logger.debug('get_write_candles: finished')


    return True



def get_limit_intervals(start_ts, end_ts, interval_ms, limit):
    # splits time from start_ts to end_ts into intervals, each interval is for one api request
    # like[[start, end], [start2, end2], ...], end - start = interval_ms*limit
    # limit = number of candles fetched per 1 API request
    # interval_ms = candle "width"
    intervals = []
    int_start = start_ts
    while int_start <= end_ts:
        int_end = int_start + interval_ms*(limit-1)
        if int_end > end_ts:
            int_end = end_ts
        intervals.append([int_start, int_end])
        int_start = int_end + 1
    return intervals

def reshape_list(lst, max_els):
    # Transform lst into a list of lists. Each sublist has length < max_els.
    # Elements order is preserved.
    if len(lst) <= max_els:
        return [lst]
    else:
        num_els = len(lst) // max_els
        leftover = len(lst) % max_els
        lst2 = [lst[i * max_els:(i + 1) * max_els] for i in range(num_els)]
        if leftover > 0:
            lst2 = lst2 + [lst[-leftover:]]
        return lst2


async def get_candles(start_ts, end_ts, client, symbol, interval, limit):
    """
        :param start_ts: starting time stamp, it seems binance has data from ['2017-08-17, 04:00:00']
        :type start_ts: int in milliseconds
        :param end_ts: ending time stamp, if None set to Now
        :type end_ts: int in milliseconds
        :param symbol: Binance symbol for example BTCUSDT
        :type symbol: string
        :type client: binance.client
        :param client: created with binance.client library
        :param interval: Binance interval string 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w
        :type interval: str
        :param limit: limit of candles returned per one api request
        :type limit: int
        writes candles or klines for a chosen symbol into a database
        candles with open_time >= start_ts included
        candles with close_time <= end_ts included
        https://python-binance.readthedocs.io/en/latest/binance.html#binance.client.Client.get_klines
        binance API returns the following klines:
        [
            [
                1499040000000,      # Open time
                "0.01634790",       # Open
                "0.80000000",       # High
                "0.01575800",       # Low
                "0.01577100",       # Close
                "148976.11427815",  # Volume
                1499644799999,      # Close time
                "2434.19055334",    # Quote asset volume
                308,                # Number of trades
                "1756.87402397",    # Taker buy base asset volume
                "28.46694368",      # Taker buy quote asset volume
                "17928899.62484339" # Can be ignored
            ]
        ]
        """
    try:
        logger.debug('start client.get_klines')
        temp_data = await client.get_klines(
            symbol=symbol,
            interval=interval,
            limit=limit,
            startTime=start_ts,
            endTime=end_ts
        )
    except asyncio.TimeoutError:
        logger.error('get_candles, client.get_klines: asyncio.TimeoutError')
        return None

    except pybin_exceptions.BinanceRequestException as err:
        logger.error(f'binance returned a non-json response, {err}')
        return None
    except pybin_exceptions.BinanceAPIException as err:
        logger.error(f'API call error, probably bad API request, details below:\n'
                     f'     response status code: {err.status_code}\n'
                     f'     response object: {err.response}\n'
                     f'     Binance error code: {err.code}\n'
                     f'     Binance error message: {err.message}\n'
                     f'     request object if available: {err.request}\n'
                     )
        return None

    if temp_data is None:
        logger.error(f'could not load data, reason unknown')
        return None
    else:

        if not len(temp_data):
            logger.warning(f'got empty response from client.get_klines(),\n'
                         f'expected candles from {start_ts} : {ts_to_date(start_ts)}'
                         f' to {ts_to_date(end_ts)} : {end_ts}')
            return []

        else:
            # insert "time_loaded" column in temp_data list of lists, set it to now()
            logger.debug('get_candles: loaded candles')
            time_loaded = int(time.time() * 1000)
            temp_data = [k + [time_loaded] for k in temp_data]
            start_temp = temp_data[0][0]
            end_temp = temp_data[-1][0]
            logger.debug('get_candles: returning candles')
    return temp_data



async def write_candles(start_ts, end_ts, client, symbol, interval, interval_ms, limit, conn_db, table_name):
    # write candles into a database
    logger.debug('write_candles: started')
    temp_data = await get_candles(start_ts, end_ts, client, symbol, interval, limit)
    # api weight is binance api load limit https://www.binance.com/ru/support/faq/360004492232
    api_weight = client.response.headers['x-mbx-used-weight-1m']

    if temp_data is None or not len(temp_data):
        return (0, 0, 0)
    else:
        try:
            # some candles have open_time not multiple of candle width, need to correct them
            # for example, while most 1 minute candles have timings like 15:00:00, some have 15:00:14
            # i[0] = open_time, i[6] = close_time
            # add [1] at the beginnig of the list if correction is going to happen
            # else add [0], in order to count corrections
            # the question is - do I really need to correct those?
            logger.debug('write_candles: got candles, transforming')
            temp_data = [
                [1] + [rdd] + i[1:6] + [rdd-i[0]+i[6]] + i[7:]
                if (rdd := interval_ms * round(i[0] / interval_ms)) != i[0]
                else
                [0] + i
                for i in temp_data]
            rounded_count = sum([i[0] for i in temp_data])
            temp_data = [i[1:] for i in temp_data]
            last_in_data = temp_data[-1][0]
            conn_db.write(temp_data, table_name)
            logger.debug(f'data written to table {table_name}')
            logger.debug('write_candles: wrote candles, returning counters')
            return (len(temp_data), rounded_count, last_in_data)
        except exceptions.SQLError:
            conn_db.close_connection()
            return 0





def get_gaps(conn_db, table_name, interval_ms, start_ts, end_ts, time_col_name='open_time'):
    logger.debug('start get_gaps')
    try:
        gaps = conn_db.get_missing(table_name, interval_ms, start_ts, end_ts)
    except exceptions.SQLError:
        conn_db.close_connection()
        return None
    logger.debug('finished get_gaps')
    return gaps











if __name__ == '__main__':
    pass

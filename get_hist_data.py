import logging
from binance.client import Client, AsyncClient
from binance import exceptions as pybin_exceptions
from db_interact import ConnectionDB
import time
import dateparser
import pytz
from datetime import datetime
import exceptions
import asyncio
import spooky

logger = logging.getLogger(__name__)


def date_to_milliseconds(date_str):
    """Convert UTC date to milliseconds
    If using offset strings add "UTC" to date string e.g. "now UTC", "11 hours ago UTC"
    See dateparse docs for formats http://dateparser.readthedocs.io/en/latest/
    :param date_str: date in readable format, i.e. "January 01, 2018", "11 hours ago UTC", "now UTC"
    :type date_str: str
    """
    # get epoch value in UTC
    epoch = datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)
    # parse our date string
    d = dateparser.parse(date_str)
    # if the date is not timezone aware apply UTC timezone
    if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
        d = d.replace(tzinfo=pytz.utc)

    # return the difference in time
    return int((d - epoch).total_seconds() * 1000.0)


def ts_to_date(ts):
    # transform a timestamp in milliseconds into a human readable date
    return datetime.utcfromtimestamp(ts / 1000).strftime("%d-%b-%Y %H:%M:%S")


def interval_to_milliseconds(interval):
    """Convert a Binance interval string to milliseconds
    :param interval: Binance interval string 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w
    :type interval: str
    :return:
         None if unit not one of m, h, d or w
         None if string not in correct format
         int value of interval in milliseconds
    """
    ms = None
    seconds_per_unit = {
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
        "w": 7 * 24 * 60 * 60
    }

    unit = interval[-1]
    if unit in seconds_per_unit:
        try:
            ms = int(interval[:-1]) * seconds_per_unit[unit] * 1000
        except ValueError:
            logger.warning(f'interval_to_milliseconds got invalid interval[:-1],'
                         f' was expecting int, got {interval[:-1]}')
    else:
        logger.error(f'interval_to_milliseconds got invalid interval unit {unit},'
                     f'valid interval units are: m, h, d, w')
    return ms


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
        temp_data = await client.get_klines(
            symbol=symbol,
            interval=interval,
            limit=limit,
            startTime=start_ts,
            endTime=end_ts
        )

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
                         f' to {end_ts} : {ts_to_date(end_ts)}')
            return []

        else:
            # insert "time_loaded" column in temp_data list of lists, set it to now()
            time_loaded = int(time.time() * 1000)
            temp_data = [k + [time_loaded] for k in temp_data]
            start_temp = temp_data[0][0]
            end_temp = temp_data[-1][0]
            #print(f'fetched {len(temp_data)} candles from {start_temp} {ts_to_date(start_temp)} included '
                  #f'to {end_temp} {ts_to_date(end_temp)} included')
    return temp_data


async def write_candles(start_ts, end_ts, client, symbol, interval, limit, conn_db, table_name):
    # write candles into a database
    temp_data = await get_candles(start_ts, end_ts, client, symbol, interval, limit)
    # api weight is binance api load limit https://www.binance.com/ru/support/faq/360004492232
    api_weight = client.response.headers['x-mbx-used-weight-1m']

    if temp_data is None or not len(temp_data):
        return 0
    else:
        try:
            conn_db.write(temp_data, table_name)
            logger.debug(f'data written to table {table_name}')
            return len(temp_data)
        except exceptions.SQLError:
            conn_db.close_connection()
            return 0


async def update_candles_ms(symbol: str, interval: str, start_ts=None, end_ts=None, limit=500, max_coroutines=50):

    # table names for each symbol and interval are created this way:
    table_name = f'{symbol}{interval}Hist'
    # initialise varaible when last entry was added to the table
    last_entry_added_at = 0
    # update candles in a database
    interval_ms = interval_to_milliseconds(interval)
    if interval_ms is None:
        logger.error('get_candles_ms got invalid interval,expected '
                     '1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w')
        return None



    # connecting to our database in order to store values
    conn_db = ConnectionDB(host=spooky.creds['host'],
                        user=spooky.creds['user'],
                        password=spooky.creds['password'],
                        database=spooky.creds['database'])
    try:
        conn_db.connect()
    except exceptions.SQLError:
        return None

    if start_ts is not None:
        logger.debug(f'start_ts specified: {start_ts}, {ts_to_date(start_ts)}')
    if end_ts is None:
        end_ts = int(time.time() * 1000)
        logger.debug(f'not specified end_ts, end ts is set to now: {end_ts}, {ts_to_date(end_ts)}')
    else:
        logger.debug(f'end_ts specified: {end_ts}, {ts_to_date(end_ts)}')


    if not conn_db.table_in_db(table_name):  # if there are no tables with table_name in our database
        logger.debug(f'not found table {table_name}, creating one')
        conn_db.table_create(table_name)
        if start_ts is None:
            start_ts = 1502668800000
            logger.debug(f'start_ts not specified, new table, setting to 1502668800000, {ts_to_date(1502668800000)}')

    else:  # table present in database
        row_count = conn_db.count_rows(table_name)
        if row_count < 1:  # table is empty
            logger.debug('update_candles_ms: found an empty table to wright in, nice!')
            if start_ts is None:
                start_ts = 1502668800000
                logger.debug(f'start_ts not specified, setting to 1502668800000, {ts_to_date(1502668800000)}')

        else:  # table not empty

            first_entry, last_entry, last_entry_added_at = conn_db.get_start_end(table_name)
            print(f'writing in existing table, first entry : {first_entry}, {ts_to_date(first_entry)}, '
                  f'last entry: {last_entry}, {ts_to_date(last_entry)}')
            print(f'latest table insertion at: {ts_to_date(last_entry_added_at)}')
            logger.debug(f'table ok, first entry: {first_entry}, {ts_to_date(first_entry)}')
            logger.debug(f'last entry: {last_entry}, {ts_to_date(last_entry)}')

            if start_ts is None:
                start_ts = last_entry + 1
                logger.debug(f'start_ts not specified, setting to {start_ts}, {ts_to_date(start_ts)}')

            if start_ts < first_entry or end_ts < last_entry:
                logger.warning('update_candles_ms: time period overlap existing candles, TRUNCATE!!!!')
                conn_db.truncate(table_name)

            # more overlapping options should be taken care of here
            else:
                if start_ts <= last_entry or start_ts > last_entry + interval_ms:
                    start_ts = last_entry + interval_ms
                    logger.debug(f'start_ts <= last_entry or start_ts > last_entry + interval_ms'
                                 f', setting to {start_ts}, {ts_to_date(start_ts)}')

    # check if requested time period is longer then "candle width"
    if start_ts is not None and end_ts is not None:
        if end_ts - start_ts < interval_ms:
            logger.warning('interval between requested start an end dates < chart interval')
            print('interval between requested start an end dates < chart interval, abort')
            return None
    print(f'going to fetch candles from {start_ts} {ts_to_date(start_ts)} to '
          f'{end_ts} {ts_to_date(end_ts)}')

    # create the Binance client, no need for api key
    client = await AsyncClient.create()

    intervals = get_limit_intervals(start_ts, end_ts, interval_ms, limit)
    resh_intervals = reshape_list(intervals, max_coroutines)

    candles_total = 0

    for bunch in resh_intervals:
        tasks = []
        for limit_interval in bunch:
            tasks.append(
                write_candles(limit_interval[0], limit_interval[1], client,
                              symbol, interval, limit, conn_db, table_name)
            )
        api_weight = client.response.headers['x-mbx-used-weight-1m']

        if int(api_weight) > 600:
            sleep_time = int(10*int(api_weight)**3/1200**3)
            logger.warning(f'reaching high api load, current api_weight:'
                           f' {api_weight}, max = 1200, sleep for {sleep_time} sec')
            time.sleep(sleep_time)
        candles_list = await asyncio.gather(*tasks)
        candles_sum = sum(candles_list)
        candles_total += candles_sum
        print(f'wrote {len(candles_list)} bunches of candles, total candles: {candles_sum}, \n'
              f'number of candles per bunch: {candles_list}')

    print('update_candles_ms summary:')
    print(f'fetched {candles_total} candles')
    first_written, last_written, count_written = conn_db.get_start_end_later_than(table_name, last_entry_added_at)
    print(f'wrote {count_written} candles starting from {first_written} {ts_to_date(first_written)} to '
          f'{last_written} {ts_to_date(last_written)}')

    conn_db.close_connection()
    await client.close_connection()

    return True


if __name__ == '__main__':
    pass

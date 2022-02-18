import asyncio
import logging
import sys
import time
from decimal import Decimal

import aiohttp
from historical_data import db_interact as db
from historical_data import exceptions
import spooky
from binance import exceptions as pybin_exceptions
from binance.client import AsyncClient, Client
from historical_data.helper_functions import interval_to_milliseconds, ts_to_date, generate_data, round_timings,\
    round_data, get_data_gaps

logger = logging.getLogger(__name__)

rounded_sum = 0
missing_sum = 0
candles_loaded = 0

# SQL table structure that will be created
candle_table_structure = [
    ['id', 'INT', 'AUTO_INCREMENT PRIMARY KEY'],
    ['open_time', 'BIGINT', 'NOT NULL UNIQUE'],
    ['open', 'DOUBLE(15,8)', 'NOT NULL'],
    ['high', 'DOUBLE(15,8)', 'NOT NULL'],
    ['low', 'DOUBLE(15,8)', 'NOT NULL'],
    ['close', 'DOUBLE(15,8)', 'NOT NULL'],
    ['volume', 'DOUBLE(25,8)', 'NOT NULL'],
    ['close_time', 'BIGINT', 'NOT NULL UNIQUE'],
    ['quote_vol', 'DOUBLE(25,8)', 'NOT NULL'],
    ['num_trades', 'BIGINT', 'NOT NULL'],
    ['buy_base_vol', 'DOUBLE(25,8)', 'NOT NULL'],
    ['buy_quote_vol', 'DOUBLE(25,8)', 'NOT NULL'],
    ['ignored', 'DOUBLE(15,8)', ''],
    ['time_loaded', 'BIGINT']
]

def get_first_timestamp(symbol: str, interval: str)->int:
    return Client()._get_earliest_valid_timestamp(symbol=symbol, interval=interval)


def get_candles_from_db(symbol: str,
                        interval: str,
                        start_ts: int,
                        end_ts: int,
                        limit=500,
                        max_coroutines=10,
                        reversed_order=True,
                        delete_existing_table=False):
    """
    :param symbol:
    :param interval:
    :param start_ts:
    :param end_ts:
    :param limit:
    :param max_coroutines:
    :return: list of lists
    returns candles with open_time fall in: start_ts <= open_time <= end_ts
    if reversed_order = True, candles are sorted from newer to older, the list be [new, old]
    it is needed to pop older first from list
    returns a list of (open_time, open, high, low, close, volume, close_time,
         quote_vol, num_trades, buy_base_vol, buy_quote_vol)
         quote_vol, num_trades, buy_base_vol, buy_quote_vol)
    exceptions scheme:
    this function raises no exceptions, tries to catch all, and returns None if exception
    interval_to_milliseconds : None
    """
    interval_ms = interval_to_milliseconds(interval)
    if interval_ms is None:
        return None

    # table names for each symbol and interval are created this way:
    table_name = f'{symbol}{interval}Hist'
    print(f'requested candles from {start_ts} {ts_to_date(start_ts)} to '
          f'{ts_to_date(end_ts)} {end_ts}')

    start_ts = max(start_ts, get_first_timestamp(symbol, interval))
    end_ts = min(end_ts, int(time.time() * 1000))

    if end_ts < start_ts: # if end_ts == start_ts, still one candle with open_time = start_ts will be returned
        logger.warning('interval between requested start an end dates < chart interval, abort')
        return None
    start_ts, end_ts = round_timings(start_ts, end_ts, interval_ms)
    print(f'going to fetch candles from {start_ts} {ts_to_date(start_ts)} to '
          f'{ts_to_date(end_ts)} {end_ts}')

    # connecting to database
    conn_db = db.ConnectionDB(host=spooky.creds['host'],
                              user=spooky.creds['user'],
                              password=spooky.creds['password'],
                              database=spooky.creds['database'])
    try:
        conn_db.connect()
    except exceptions.SQLError:
        return None


    if conn_db.table_in_db(table_name) and delete_existing_table: #  table present and delete flag is on
        conn_db.table_delete(table_name)

    #  check if table present, else create
    if not conn_db.table_in_db(table_name):
        logger.debug(f'not found table {table_name}, creating one')
        if not conn_db.table_create(table_name, candle_table_structure):
            conn_db.close_connection()
            return None



    # get when last entry was added to the table, need this for printing report of how many candles were written
    # in case of error returns 0
    last_entry_id = conn_db.get_latest_id(table_name)

    # check if requested data is present in database
    # search for gaps in interval from start_ts to end_ts, gap = absence of data in database
    # gaps is a list of tuples [(gap1_start, gap1_end), (gap2_start, gap2_end), ...]
    # returns None in case of error
    gaps = get_gaps(conn_db, table_name, interval_ms, start_ts, end_ts)
    if gaps is None: # get_gaps function encountered an error
        logger.error('get_gaps function returned None')
        conn_db.close_connection()
        return None
    if gaps: #if there are gaps in a period of interest
        print(' found gaps:')
        for gap in gaps:
            print(f' {gap[0]} {ts_to_date(gap[0])} - {ts_to_date(gap[1])} {gap[1]}')
        for gap in gaps:
            print(f'  loading gap from exchange: {gap[0]} {ts_to_date(gap[0])} - {ts_to_date(gap[1])} {gap[1]}')

            # get candles covering gap from exchange and write them in the database
            if not asyncio.run(get_write_candles(conn_db, symbol, table_name, interval, interval_ms, gap[0], gap[1],
                                                 limit, max_coroutines)):
                logger.error('get_write_candles failed.')
                conn_db.close_connection()
                return None


    _, _, count_written = conn_db.get_start_end_later_than(table_name, last_entry_id, only_count=True)
    if count_written:
        print(f'wrote {count_written} candles to db')

    #  ok, we tried to get data from exchange, now just fetch from database:
    logger.debug('reading from db: conn_db.read()')
    fetch = conn_db.read(table_name, start_ts, end_ts, reversed_order=reversed_order)
    if fetch:
        print(f'fetched {len(fetch)} candles from the database')

    try:
        conn_db.close_connection()
    except exceptions.SQLError:
        logger.error('failed to close connection to a database')

    return fetch



async def get_write_candles(conn_db, symbol, table_name, interval: str, interval_ms: int, start_ts, end_ts,
                            limit, max_coroutines):
    #  return None if fails, else return True

    # get when last entry was added to the table, need this to print out a final report
    last_entry_id = conn_db.get_latest_id(table_name)

    #  Create the Binance client, no need for api key.

    try:
        exchange_client = await AsyncClient.create(requests_params={"timeout": 60})
    except aiohttp.client_exceptions.ClientConnectorCertificateError as err:
        logger.error(f'ClientConnectorCertificateError: {err}, try importing aiohttp prior to everything else')
        return None

    #  break the timeline of interest into periods, each period will get it's own concurrent worker
    periods = get_limit_intervals(start_ts, end_ts, interval_ms, limit)

    #  If we have too many concurrent requests at the same time, some of them get timeouted.
    #  In order to handle theese, we run timeouted requests again. And again. Unless there is decrease in their number.
    #  Actually, this is an overkill, because if we don't go with more then 50 concurrent tasks at a time,
    #  it is very unlikely to get a timeout error.
    #  I just wanted to be shure I squeezed every little piece of data out of the exchange.
    i = 1
    while True:
        timeout_gaps = await gather_write_candles(periods, max_coroutines, exchange_client,
                             symbol, interval, interval_ms, limit, conn_db, table_name)
        if (
                not timeout_gaps or  #  Stop cycle if we recieve no timeout errors from exchange.
                len(timeout_gaps) == len(periods) or  #  or if recieve same number of timeouts as the last iteration.
                i > 10  #  Just in case. Just to be on the safe side. I am afraid of getting stuck in infinite loops.
        ): break
        print(f'   Got timeouts from exchange, going to iterate one more time. Iteration N {i}.\n'
              f'   Consider lowering max_coroutines parameter in get_candles_from_db function.')
        i += 1
        periods = timeout_gaps


    print('   gap summary:')
    first_written, last_written, count_written = conn_db.get_start_end_later_than(table_name, last_entry_id)
    if first_written and last_written:
        print(f'    wrote to db {count_written} candles starting from {first_written} {ts_to_date(first_written)} to '
              f'{ts_to_date(last_written)} {last_written}')
    else:
        print(f'   wrote {count_written} candles')

    await exchange_client.close_connection()

    return True



async def gather_write_candles(periods, max_coroutines, exchange_client,
                          symbol, interval, interval_ms, limit, conn_db, table_name):
    sem = asyncio.Semaphore(max_coroutines)
    async def sem_task(task):
        async with sem:
            return await task

    tasks = [
        sem_task(
            write_candles(period[0], period[1], exchange_client,
                          symbol, interval, interval_ms, limit, conn_db, table_name)
        )
        for period in periods
    ]

    gathered_result = await asyncio.gather(*tasks)
    timeout_gaps = [result[1] for result in gathered_result if result[1]]

    good_bunches = sum([1 for i in gathered_result if i[0]])
    error_bunches = sum([1 for i in gathered_result if i[0] is None])
    count_timeout_gaps = sum([1 for i in gathered_result if i[1]])
    print(f'\n   total bunches: {len(gathered_result)}, '
          f'errorless bunches: {good_bunches}, bunches with errors: {error_bunches}, '
          f'timeout gaps: {count_timeout_gaps}')

    return timeout_gaps




async def write_candles(start_ts, end_ts, client, symbol, interval,
                        interval_ms, limit, conn_db, table_name):
    # write candles into a database
    temp_data, timeout_gap = await get_candles(start_ts, end_ts, client, symbol, interval, interval_ms, limit)

    try:
        api_weight = client.response.headers['x-mbx-used-weight-1m']
    except AttributeError as err:
        logger.error(f'getting api weight, got: {err}')
        api_weight = 599
    except KeyError as err:
        api_weight = 599
        logger.error(f'getting api weight, probably got response != 200. Error is: {err}')

    if int(api_weight) > 600:
        sleep_time = int(10 * int(api_weight) ** 3 / 1200 ** 3)
        logger.warning(f'reaching high api load, current api_weight:'
                       f' {api_weight}, max = 1200, sleep for {sleep_time} sec')
        time.sleep(sleep_time)


    if temp_data is None or not len(temp_data):
        return (None, timeout_gap)
    else:
        if conn_db.write(temp_data, table_name, candle_table_structure):
            return (True, timeout_gap)
        else:
            return (None, timeout_gap)


# format data to be written into the table
# not used anywhere, lol
def adapt_data(data):
    adapted = [[j[0],  # open_time
                Decimal(j[1]),  # open
                Decimal(j[2]),  # high
                Decimal(j[3]),  # low
                Decimal(j[4]),  # close
                Decimal(j[5]),  # volume
                j[6],  # close_time
                Decimal(j[7]),  # quote_vol
                j[8],  # num_trades
                Decimal(j[9]),  # buy_base_vol
                Decimal(j[10]),  # buy_quote_vol
                Decimal(j[11]),  # ignored
                j[12]  # time loaded
                ] for j in data]
    return adapted


async def get_candles(start_ts, end_ts, client, symbol, interval, interval_ms, limit):
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
        candles with open_time <= end_ts included
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
    temp_data = None
    rounded_count = 0
    global rounded_sum
    global candles_loaded
    global missing_sum
    timeout_gap = []

    try:
        # if start_ts = end_ts, this will return one candle with open_time = start_ts
        temp_data = await client.get_klines(
            symbol=symbol,
            interval=interval,
            limit=limit,
            startTime=start_ts,
            endTime=end_ts
        )
    except asyncio.TimeoutError as err:
        logger.warning(f'get_candles, client.get_klines: asyncio.TimeoutError; {err}')
        timeout_gap = [start_ts, end_ts]

    except pybin_exceptions.BinanceRequestException as err:
        logger.error(f'binance returned a non-json response, {err}')

    except pybin_exceptions.BinanceAPIException as err:
        logger.error(f'API call error, probably bad API request, details below:\n'
                     f'     response status code: {err.status_code}\n'
                     f'     response object: {err.response}\n'
                     f'     Binance error code: {err.code}\n'
                     f'     Binance error message: {err.message}\n'
                     f'     request object if available: {err.request}\n'
                     )

    except Exception as err:
        logger.error(f'get_candles: unknown error: {err}')

    if temp_data is not None:

        temp_data, rounded_count = round_data(temp_data, interval_ms)
        rounded_sum += rounded_count

        # if sent correct request without raising any errors, check for gaps in data
        # if there are any gaps, write expected candles with -1 in all fields except for open_time and close_time
        response_status = client.response.status

        if response_status == 200:
            temp_gaps = get_data_gaps(temp_data, start_ts, end_ts, interval_ms)
            shift = 0
            for gap in temp_gaps:
                insert_index = gap[2] + shift
                generated = generate_data(gap[0], gap[1], interval_ms)
                missing_sum += len(generated)
                temp_data[insert_index:insert_index] = generated
                shift += len(generated)

        #  insert "time_loaded" column in temp_data list of lists, set it to now()
        time_loaded = int(time.time() * 1000)
        temp_data = [k + [time_loaded] for k in temp_data]

        last_in_data = temp_data[-1][0]
        candles_loaded += len(temp_data)

        str1 = "\r   last candle:{0}, candles loaded:{1}, rounded:{2}, missing:{3}, tasks remain:{4}".format(
            ts_to_date(last_in_data),
            candles_loaded,
            rounded_sum,
            missing_sum,
            len(asyncio.all_tasks())-2
        )
        sys.stdout.write(str1)


    else:
        logger.error(f'could not load data')

    return (temp_data, timeout_gap)




def get_limit_intervals(start_ts, end_ts, interval_ms, limit):
    # splits time from start_ts to end_ts into intervals, each interval is for one api request
    # like[[start, end], [start2, end2], ...], end - start = interval_ms*limit
    # limit = number of candles fetched per 1 API request
    # interval_ms = candle "width"
    intervals = []
    int_start = start_ts
    while int_start <= end_ts:
        int_end = int_start + interval_ms*(limit-1) - 1
        if int_end > end_ts:
            int_end = end_ts
        intervals.append([int_start, int_end])
        int_start = int_end + 1
    return intervals



def get_gaps(conn_db, table_name, interval_ms, start_ts, end_ts, time_col_name='open_time'):
    logger.debug('start get_gaps')

    gaps = conn_db.get_missing(table_name, interval_ms, start_ts, end_ts)

    if not gaps and gaps is not None: #if we got an empty list but not a None
        print('all requested data already present in database, no gaps found')
    logger.debug('end get_gaps')
    return gaps





if __name__ == '__main__':
    pass

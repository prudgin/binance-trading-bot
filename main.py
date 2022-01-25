# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import logging
import sys
import gc
import tracemalloc
import time
import get_hist_data as ghd  # aiohttp should be imported before mysql.connector
import db_interact as db
import helper_functions as hlp

import spooky

logging.basicConfig(
    # filename='get_hist_data.log',
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.WARNING,
    datefmt="%H:%M:%S",
    stream=sys.stderr
)

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    conn_creds = {
        'host': spooky.creds['host'],
        'user': spooky.creds['user'],
        'password': spooky.creds['password'],
        'database': spooky.creds['database']
    }
    conn_db = db.ConnectionDB(**conn_creds)

    # print(conn)
    conn_db.connect()

    # print(conn_db.list_databases())
    conn_db.table_delete('BTCUSDT1dHist')

    conn_db.close_connection()

    # 1502668800000 default start
    # end_ts = int(time.time() * 1000) = now
    start = time.perf_counter()

    # https://stackoverflow.com/questions/1316767/how-can-i-explicitly-free-memory-in-python

    tracemalloc.start()

    fetch = ghd.get_candles_from_db('BTCUSDT', '1d', start_ts=1543104000000,
                                    end_ts=1543104000000 - 1 + 60000 * 60 * 10)
    for candle in fetch:
        print(f'id: {candle[0]}; '
              f'open: {hlp.ts_to_date(candle[1])}; '
              f'closed: {hlp.ts_to_date(candle[7])}; '
              f'len: {round((candle[7] - candle[1]) / (1000 * 60 * 60 * 24))}; '
              f'added: {hlp.ts_to_date(candle[-1])}')
        print(candle)
        print(type(candle))

    # del fetched_candles
    # gc.collect()

    current, peak = tracemalloc.get_traced_memory()
    print(f"Current memory usage is {current / 10 ** 6}MB; Peak was {peak / 10 ** 6}MB")
    tracemalloc.stop()

    print(f'it took {time.perf_counter() - start}')

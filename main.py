# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from get_hist_data import update_candles_ms, interval_to_milliseconds, ts_to_date
import logging
import sys
import asyncio
import spooky
import exceptions
from check_table import check_table, round_timings

logging.basicConfig(
    # filename='get_hist_data.log',
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.WARNING,
    datefmt="%H:%M:%S",
    stream=sys.stderr
)

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    from db_interact import ConnectionDB

    conn_db = ConnectionDB(host=spooky.creds['host'],
                        user=spooky.creds['user'],
                        password=spooky.creds['password'],
                        database=spooky.creds['database'])

    #print(conn)
    conn_db.connect()

    #print(conn_db.list_databases())
    #conn_db.table_delete('BTCUSDT1mHist')
    conn_db.close_connection()


    asyncio.run(update_candles_ms('BTCUSDT', '1m',start_ts=1512086400000, end_ts=1519862400000))
    round_timings('BTCUSDT1mHist', '1m')
    check_table('BTCUSDT1mHist', '1m')

    #1502942400000: 17 - Aug - 2017 04: 00:00, 1515927960000: 14 - Jan - 2018 11: 06:00

#604800000
#86400000

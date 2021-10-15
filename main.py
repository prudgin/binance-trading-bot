# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import logging
import sys
import time
import asyncio
import spooky
import exceptions
from get_hist_data import get_candles_from_db
from db_interact import ConnectionDB

logging.basicConfig(
    # filename='get_hist_data.log',
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
    stream=sys.stderr
)

logger = logging.getLogger(__name__)

if __name__ == '__main__':


    conn_db = ConnectionDB(host=spooky.creds['host'],
                        user=spooky.creds['user'],
                        password=spooky.creds['password'],
                        database=spooky.creds['database'])

    #print(conn)
    conn_db.connect()

    #print(conn_db.list_databases())
    #conn_db.table_delete('BTCUSDT1mHist')
    conn_db.close_connection()

    #1502668800000 default start
    #end_ts = int(time.time() * 1000) = now
    get_candles_from_db('BTCUSDT', '1m', start_ts=1502668800000, end_ts=int(time.time() * 1000))




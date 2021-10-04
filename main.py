# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from get_hist_data import update_candles_ms
import logging
import sys
import asyncio

logging.basicConfig(
    # filename='get_hist_data.log',
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
    stream=sys.stderr
)

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    from db_interact import ConnectionDB

    conn = ConnectionDB(host='localhost',
                        user='trading',
                        password='spooky45',
                        database='hist_data')

    #print(conn)
    conn.connect()
    #print(conn.list_databases())

    conn.table_delete('BTCUSDT1dHist')
    conn.close_connection()
    #update_candles_ms('BTCUSDT', '1d', 1502928000000)#, 1502928000000 + 86400000*7, limit = 7)
    asyncio.run(update_candles_ms('BTCUSDT', '1d', 1502928000000))

#604800000
#86400000

from get_hist_data import update_candles_ms
import logging
import sys
import asyncio
import spooky
import exceptions

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    from db_interact import ConnectionDB

    conn = ConnectionDB(host=spooky.creds['host'],
                        user=spooky.creds['user'],
                        password=spooky.creds['password'],
                        database=spooky.creds['database'])

    #print(conn)
    conn.connect()
    #print(conn.list_databases())

    #conn.table_delete('BTCUSDT1wHist')
    try:
        conn.add_column('BTCUSDT1wHist', 'checked', 'INT', 'NOT NULL')
    except exceptions.SQLError as err:
        print(err)

    conn.close_connection()
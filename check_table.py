from get_hist_data import update_candles_ms, interval_to_milliseconds
import logging
import sys
import asyncio
import spooky
import exceptions
import pandas as pd
from db_interact import ConnectionDB

logger = logging.getLogger(__name__)

def check_table(table_name):

    # connecting to our database in order to store values
    conn_db = ConnectionDB(host=spooky.creds['host'],
                           user=spooky.creds['user'],
                           password=spooky.creds['password'],
                           database=spooky.creds['database'])
    try:
        conn_db.connect()
    except exceptions.SQLError:
        return None

    try:
        conn_db.add_column(table_name, 'checked', 'INT')
    except exceptions.SQLError as err:
        logger.error(f'failed to insert column, what a shame!, {err}')
        return None

    missing = conn_db.check_missing(table_name, 'open_time', interval_to_milliseconds('1m'))

    conn_db.close_connection()
    return missing


if __name__ == '__main__':



    print(check_table('BTCUSDT1mHist'))



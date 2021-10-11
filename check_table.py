from get_hist_data import update_candles_ms, interval_to_milliseconds, ts_to_date
from mysql.connector import connect, Error, errors
import logging
import sys
import asyncio
import spooky
import exceptions
import pandas as pd
from db_interact import ConnectionDB

logger = logging.getLogger(__name__)

def set_checked_1(table_name, cursor, connection):
    # checked = 0 or NULL if timing was not checked for being multiple of candle width
    # checked = 1 if was checked
    request = f"""
            UPDATE {table_name}      
             SET checked = 1
            WHERE (checked IS NULL) OR (checked = 0)
            """
    cursor.execute(request)
    connection.commit()
    updated = cursor.rowcount
    if updated > 0:
        print(f'set checked = 1 for {updated} candles')
    cursor.reset()


def round_timings(table_name, interval, time_col_name='open_time', close_name='close_time'):
    # search for candles with open time not multiple of candle width and correct them
    interval_ms = interval_to_milliseconds(interval)
    if interval_ms is None:
        logger.error('get_candles_ms got invalid interval,expected '
                     '1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w')
        return None

    conn_db = ConnectionDB(host=spooky.creds['host'],
                           user=spooky.creds['user'],
                           password=spooky.creds['password'],
                           database=spooky.creds['database'])

    conn_db.connect()

    connection, cursor = conn_db.get_db_and_cursor()

    request = f"""
    SELECT COUNT(*) FROM {table_name}
    WHERE ((checked IS NULL) OR (checked = 0)) AND (MOD({time_col_name}, {interval_ms}) > 0)
    """
    cursor.execute(request)
    fetch = cursor.fetchone()[0]
    cursor.reset()
    if fetch > 0:
        print(f'found {fetch} candles with open time not multiple of candle width')
    else:
        set_checked_1(table_name, cursor, connection)
        conn_db.close_connection()
        return None

    request = f"""
    UPDATE IGNORE {table_name}      
     SET
     {time_col_name} = CAST({interval_ms}*ROUND(1.0*{time_col_name}/{interval_ms}) AS UNSIGNED INTEGER),
     {close_name} = CAST({interval_ms}*ROUND(1.0*{close_name}/{interval_ms}) AS UNSIGNED INTEGER),
     rounded = 1
    WHERE (MOD({time_col_name}, {interval_ms}) > 0) AND ((checked IS NULL) OR (checked = 0))
    """
    cursor.execute(request)
    connection.commit()
    updated = cursor.rowcount
    if updated > 0:
        print(f'updated {updated} candles with open and close time rounded'
              f' to the closest time multiple of candle width')
    cursor.reset()

    request = f"""
        DELETE FROM {table_name}      
        WHERE (MOD({time_col_name}, {interval_ms}) > 0) AND ((checked IS NULL) OR (checked = 0))
        """
    cursor.execute(request)
    connection.commit()
    deleted = cursor.rowcount
    if deleted > 0:
        print(f'deleted {deleted} candles with open time not multiple of candle width')
    cursor.reset()

    set_checked_1(table_name, cursor, connection)

    conn_db.close_connection()




def get_gaps(table_name, interval, start_ts, end_ts,  time_col_name='open_time'):
    interval_ms = interval_to_milliseconds(interval)
    if interval_ms is None:
        logger.error('get_candles_ms got invalid interval,expected '
                     '1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w')
        return None

    conn_db = ConnectionDB(host=spooky.creds['host'],
                           user=spooky.creds['user'],
                           password=spooky.creds['password'],
                           database=spooky.creds['database'])
    try:
        conn_db.connect()
    except exceptions.SQLError:
        return None

    try:
        gaps = conn_db.get_missing(table_name, interval_ms, start_ts, end_ts, time_col_name='open_time')
    except exceptions.SQLError:
        conn_db.close_connection()
        return None
    conn_db.close_connection()
    return gaps






if __name__ == '__main__':

    pass
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


def round_timings(table_name, interval, time_col_name='open_time', close_name='close_time'):

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
    WHERE (MOD({time_col_name}, {interval_ms}) > 0) AND ((checked IS NULL) OR (checked = 0))
    """
    cursor.execute(request)
    fetch = cursor.fetchone()[0]
    cursor.reset()
    if fetch > 0:
        print(f'found {fetch} candles with open time not multiple of candle width')

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

    conn_db.close_connection()


def check_table(table_name, interval, time_col_name='open_time', close_name='close_time'):

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

    connection, cursor = conn_db.get_db_and_cursor()

    try:
        cursor.execute(f"""
            SELECT CAST((z.expected) AS UNSIGNED INTEGER),
                   IF(z.got-{interval_ms}>z.expected, CAST((z.got-{interval_ms}) AS UNSIGNED INTEGER),
                   CAST((z.expected) AS UNSIGNED INTEGER)) AS missing
            FROM
               (SELECT 
                /* (3) increace @rownum by step from row to row */
                @rownum:=@rownum+{interval_ms} AS expected, 
                
                /* (4) @rownum should be equal to time_col_name unless we find a gap, 
                       when we do, overwrite @rownum with gap end and continue*/ 
               IF(@rownum={time_col_name}, 0, @rownum:={time_col_name}) AS got
               FROM 
                /* (1) set variable @rownum equal to the first entry in sorted time_col_name */
                 (SELECT @rownum:=
                    (SELECT {time_col_name}
                     FROM {table_name}
                     WHERE ((CHECKED IS NULL) OR (CHECKED < 2))/*choose only rows not checked previously*/
                     ORDER BY {time_col_name}
                     LIMIT 1)-{interval_ms}) AS a 
               /* (2) join a column populated with variable @rownum, 
                      for now it doesn't change from row to row */
               JOIN
                 (SELECT *
                  FROM {table_name}
                  WHERE ((CHECKED IS NULL) OR (CHECKED < 2)) ) AS b
               ORDER BY {time_col_name}) AS z
            WHERE z.got!=0;
        """)
        missing = cursor.fetchall()
        cursor.reset()

    except Error as err:
        err_message = 'error searching for gaps in table'
        logger.error(f'{err_message} {table_name}, {err}')
        return None


    print('gaps:')
    print(missing)

    for gap in missing:
        print(f'gap: {ts_to_date(gap[0])} - {ts_to_date(gap[1])}')
        read_gap = conn_db.read(table_name, 'open_time', gap[0] - interval_ms, gap[1] + interval_ms)
        if len(read_gap) != 2:
            logger.error(f'check_table found a strange gap: {[ts_to_date(i[1]) for i in read_gap]}')
        for subgap in read_gap:
            print(f'subgap: {subgap[1]}, {ts_to_date(subgap[1])}')


    request = f"""
        /* keep the latest entry in order to check for gaps later between it and new entries*/
        
        UPDATE {table_name} AS t, 
               (SELECT MAX({time_col_name}) AS {time_col_name} FROM {table_name}) AS m      
        SET t.checked = 2
        WHERE ((CHECKED IS NULL) OR (CHECKED < 2)) AND
              (t.{time_col_name} != m.{time_col_name})     
        """
    cursor.execute(request)
    connection.commit()
    updated = cursor.rowcount
    if updated > 0:
        print(f'set checked = 2 for {updated} candles')
    cursor.reset()



    conn_db.close_connection()
    return missing



if __name__ == '__main__':

    pass
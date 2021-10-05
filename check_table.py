from get_hist_data import update_candles_ms
import logging
import sys
import asyncio
import spooky
import exceptions
import pandas as pd

logging.basicConfig(
    # filename='get_hist_data.log',
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
    stream=sys.stderr
)

logger = logging.getLogger(__name__)

def check_table(conn_db, table_name):
    try:
        conn_db.add_column(table_name, 'checked', 'INT', 'NOT NULL')
    except exceptions.SQLError as err:
        logger.error(f'failed to insert column, what a shame!, {err}')
        return None

    _, cursor = conn_db.get_db_and_cursor()

    cursor.execute(f"""
                SELECT open_time, open, high, low, close, time_loaded, checked,
                LAG(open_time) OVER (ORDER BY open_time) AS prev_time
                FROM {table_name}
                WHERE (checked IS NULL OR checked = '' OR checked = 0)
                ORDER BY open_time
                """)
    fetch = cursor.fetchall()
    cursor.reset()

    df = pd.DataFrame(fetch, columns=['open_time', 'open', 'high', 'low',
                                      'close', 'time_loaded', 'checked', 'prev_time'])
    df = df.astype({'prev_time': 'Int64'})

    pd.set_option('display.max_rows', None)
    print(df)

if __name__ == '__main__':
    from db_interact import ConnectionDB

    conn_db = ConnectionDB(host=spooky.creds['host'],
                        user=spooky.creds['user'],
                        password=spooky.creds['password'],
                        database=spooky.creds['database'])

    conn_db.connect()

    check_table(conn_db, 'BTCUSDT1wHist')


    conn_db.close_connection()
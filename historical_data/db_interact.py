import logging

from historical_data import exceptions
import mysql.connector as mysql_connector
from historical_data import helper_functions as helpers

logger = logging.getLogger(__name__)

# this class is used to connect to a MySQL database
class ConnectionDB:
    def __init__(self, host, user, password, database=None):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.table = None
        self.conn = None
        self.conn_cursor = None
        self.connected = False

    def __str__(self):
        return f'This is an instance of ConnectionDB class,' \
               f'database = {self.database}'

    def list_databases(self):
        try:
            with mysql_connector.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute('SHOW DATABASES')
                    db_list = cursor.fetchall()
        except mysql_connector.Error as e:
            logger.error(f'error while doing list_databases() {e}')
        mysql_system_dbs = [('information_schema',), ('mysql',), ('performance_schema',), ('sys',)]
        return [db[0] for db in db_list if db not in mysql_system_dbs]

    def connect(self):
        try:
            self.conn = mysql_connector.connect(host=self.host,
                                                user=self.user,
                                                password=self.password,
                                                database=self.database)
        except mysql_connector.Error as err:
            err_message = 'error while connecting to a database'
            logger.error(f'{err_message} {self.database}; {err}')
            raise exceptions.SQLError(err, err_message)
        else:
            self.conn_cursor = self.conn.cursor()
            logger.debug(f'connected to database {self.database}')
            self.connected = True

    def close_connection(self):
        if self.connected:
            try:
                self.conn_cursor.close()
                self.conn.close()
                self.connected = False
                logger.debug(f'closed connection to database {self.database}')
            except mysql_connector.Error as err:
                err_message = 'error while closing connection to a database'
                logger.error(f'{err_message} {self.database}; {err}')
                raise exceptions.SQLError(err, err_message)


    def get_db_and_cursor(self):
        if self.connected:
            return [self.conn, self.conn_cursor]
        else:
            return [None, None]

    def table_create(self, table_name, data_structure):
        """
        :param table_name: name of the table to be created
        :param data_structure: table structure e.g.:
        [
            ['id', 'INT', 'AUTO_INCREMENT PRIMARY KEY'],
            ['open_time', 'BIGINT', 'NOT NULL UNIQUE'],
            ...
        ]
        """
        if not self.connected:
            logger.error('cannot create table, not connected to a database, run .connect() first')
            return None
        else:
            try:
                table_struct_string = ',\n'.join([' '.join(i) for i in data_structure])
                create_new_table_query = f"""
                        CREATE TABLE {table_name} (
                        {table_struct_string}
                        )
                        """
                self.conn_cursor.execute(create_new_table_query)
                self.conn.commit()
            except mysql_connector.Error as err:
                logger.error(f'could not create table {table_name}, {err}')
                return None
        return True

    def table_delete(self, table_name):
        if self.table_in_db(table_name):
            try:
                delete_table_query = f'DROP TABLE {table_name}'
                self.conn_cursor.execute(delete_table_query)
                self.conn.commit()
                logger.debug(f'deleted table {table_name}')
            except mysql_connector.Error as err:
                err_message = 'unknown error while deleting a table'
                logger.error(f'{err_message}; {self.database}; {err}')
                raise exceptions.SQLError(err, err_message)
        else:
            logger.error(f'could not delete table {table_name}, no such table')



    def table_in_db(self, table_name):
        show_req = f'SHOW TABLES LIKE \'{table_name}\''
        tables = None
        try:
            self.conn_cursor.execute(show_req)
            tables = self.conn_cursor.fetchall()
        except mysql_connector.Error as err:
            logger.error(f'failed to table_in_db, {err}')
            return None
        if tables:
            return True

    def list_tables(self):
        show_req = f'SHOW TABLES'
        tables = None
        try:
            self.conn_cursor.execute(show_req)
            tables = self.conn_cursor.fetchall()
        except mysql_connector.Error as err:
            logger.error(f'failed to list_tables, {err}')
            return None
        return tables

    def count_rows(self, table_name):
        try:
            count_req = f'SELECT COUNT(open_time) FROM {table_name}'
            self.conn_cursor.execute(count_req)
            return self.conn_cursor.fetchone()[0]
        except mysql_connector.Error as err:
            logger.error(f'failed to count_rows, {err}')
            return None

    def list_columns(self, table_name):
        request = f"""
                    SHOW COLUMNS FROM {self.database}.{table_name}
        """
        self.conn_cursor.execute(request)
        fetch = self.conn_cursor.fetchall()
        self.conn_cursor.reset()
        return [i[0] for i in fetch]

    def add_column(self, table_name, column_name, col_type='', not_null='', unique=''):
        if column_name in self.list_columns(table_name):
            logger.debug(f'tried to insert a duplicate column{column_name}')
            return None
        add_req = f"""
                ALTER TABLE {table_name}
                ADD COLUMN {column_name} {col_type} {not_null} {unique}
                """
        add_req = ' '.join(add_req.split())
        try:
            self.conn_cursor.execute(add_req)
        except mysql_connector.errors.ProgrammingError as err:
            if 'Duplicate column name' in str(err):
                logger.debug(f'tried to insert a duplicate column{column_name}')
        except mysql_connector.Error as err:
            err_message = 'failed to insert column'
            logger.error(f'{err_message}, {err}')
            raise exceptions.SQLError(err, err_message)

    def get_latest_id(self, table_name):
        # return the id of entry that was added last
        last_id = 0
        try:
            last_time = f"""
                            SELECT id FROM {table_name}
                            ORDER BY id DESC
                            LIMIT 1
                            """
            self.conn_cursor.execute(last_time)
            fetch = self.conn_cursor.fetchone()
            self.conn_cursor.reset()
            if fetch:
                last_id = fetch[0]
        except mysql_connector.Error as err:
            logger.error(f'failed to get last id from table {table_name}, {err}')
        return last_id


    def get_start_end_later_than(self, table_name, later_than, only_count=False):
        # get the timestamp of the first and the last entry in existing table added after "later_than"
        # also count entries
        first_entry, last_entry, count = None, None, 0

        try:
            count_req = f'SELECT COUNT(id) FROM {table_name} WHERE id > {later_than}'
            self.conn_cursor.execute(count_req)
            count = self.conn_cursor.fetchone()[0]
            self.conn_cursor.reset()
            if only_count:
                return [first_entry, last_entry, count]
        except mysql_connector.Error as err:
            logger.error(f'failed to count_rows, {err}')
            return [first_entry, last_entry, count]

        if count < 1:  # if table has no entries later than specified time
            logger.debug(f'tried to get first and last entry from table that'
                         f' has no entries later than specified time {table_name}')
            return [first_entry, last_entry, count]
        else:
            try:
                first_req = f"""SELECT open_time from {table_name}
                                WHERE id > {later_than}
                                ORDER BY open_time LIMIT 1"""
                self.conn_cursor.execute(first_req)
                first_entry = self.conn_cursor.fetchone()[0]
                self.conn_cursor.reset()
                last_req = f"""SELECT open_time from {table_name}
                               WHERE id > {later_than}
                               ORDER BY open_time DESC LIMIT 1"""
                self.conn_cursor.execute(last_req)
                last_entry = self.conn_cursor.fetchone()[0]
                self.conn_cursor.reset()

            except mysql_connector.Error as err:
                logger.error(f'failed to get first and last entry from table {table_name}, {err}')

        return [first_entry, last_entry, count]




    def write(self, data, table_name, data_structure):
        headers_list = [i[0] for i in data_structure][1:]
        headers_string = ', '.join(headers_list)
        helper_string = ' ,'.join(['%s'] * len(headers_list))
        insert_req = f"""
                INSERT IGNORE INTO {table_name}
                ({headers_string})
                VALUES ( {helper_string})
                """
        try:
            self.conn_cursor.executemany(insert_req, data)
            self.conn.commit()
        except mysql_connector.Error as err:
            err_message = 'error writing data into table'
            logger.error(f'{err_message} {table_name}, {err}')
            raise exceptions.SQLError(err, err_message)

    def read(self, table_name, start_ts, end_ts, time_col_name='open_time', reversed_order=True):
        """returns candles from database in a form of
        (open_time, open, high, low, close, volume, close_time,
         quote_vol, num_trades, buy_base_vol, buy_quote_vol)"""
        if reversed_order:
            order = 'DESC'
        else:
            order = 'ASC'
        read_req = f"""
                SELECT open_time, open, high, low, close, volume, close_time,
         quote_vol, num_trades, buy_base_vol, buy_quote_vol FROM {table_name}
                WHERE {time_col_name} BETWEEN {start_ts} AND {end_ts}
                ORDER BY {time_col_name} {order};
                """
        try:
            self.conn_cursor.execute(read_req)
            fetch = self.conn_cursor.fetchall()
            self.conn_cursor.reset()

            return fetch
        except mysql_connector.Error as err:
            err_message = 'error reading data from table'
            logger.error(f'{err_message} {table_name}, {err}')
            raise exceptions.SQLError(err, err_message)



    def get_missing(self, table_name, interval_ms, start, end,  time_col_name='open_time'):


        if self.check_number_candles_in_period(table_name, interval_ms, start, end, time_col_name='open_time'):
            return []  # All candles in place, have no gaps.

        missing = None
        last_entry = None
        # get the last entry in start-end range
        request = f"""
        SELECT MAX({time_col_name}) FROM {table_name}
        WHERE {time_col_name} BETWEEN {start} AND {end}
        """
        try:
            self.conn_cursor.execute(request)
            last_entry = self.conn_cursor.fetchone()[0]
            self.conn_cursor.reset()
        except mysql_connector.Error as err:
            err_message = 'get gaps, error finding last entry'
            logger.error(f'{err_message} {table_name}, {err}')
            raise exceptions.SQLError(err, err_message)

        #if there are no records between start and end
        if last_entry is None:
            return([(start, end)])

        # find gaps
        try:
            self.conn_cursor.execute(f"""
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
                    /* (1) set variable @rownum equal to start */
                     (SELECT @rownum:={start}-{interval_ms}) AS a 
                   /* (2) join a column populated with variable @rownum, 
                          for now it doesn't change from row to row */
                   JOIN
                     (SELECT *
                      FROM {table_name}
                      WHERE {time_col_name} BETWEEN {start} AND {end}
                      ) AS b
                   ORDER BY {time_col_name}) AS z
                WHERE z.got!=0;
            """)
            missing = self.conn_cursor.fetchall()
            self.conn_cursor.reset()


        except mysql_connector.Error as err:
            err_message = 'error searching for gaps in table'
            logger.error(f'{err_message} {table_name}, {err}')
            raise exceptions.SQLError(err, err_message)

        #  in case we have empty space between last entry and an "end" we have requested
        if last_entry < end:
            missing.append((last_entry + interval_ms, end))



        return(missing)

    def check_number_candles_in_period(self, table_name, interval_ms, start, end, time_col_name='open_time'):
        start, end = helpers.round_timings(start, end, interval_ms)
        #  Count number of candles between start and end. If all candles are in place, return True.
        request = f"""
                SELECT COUNT({time_col_name}) FROM {table_name}
                WHERE {time_col_name} BETWEEN {start} AND {end}
                """
        try:
            self.conn_cursor.execute(request)
            entry_count = self.conn_cursor.fetchone()[0]
            self.conn_cursor.reset()
        except mysql_connector.Error as err:
            err_message = 'count entries error'
            logger.error(f'{err_message}, {table_name}, {err}')
            return None
        if entry_count == (end - start) / interval_ms + 1:
            return True
        return None



if __name__ == '__main__':
    pass

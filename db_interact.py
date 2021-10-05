from mysql.connector import connect, Error
from getpass import getpass
import logging
import sys
from decimal import Decimal
import exceptions

logger = logging.getLogger(__name__)

# SQL table structure that will be created
candle_table_structure = [
    ['id', 'INT', 'AUTO_INCREMENT PRIMARY KEY'],
    ['open_time', 'BIGINT', 'NOT NULL UNIQUE'],
    ['open', 'DECIMAL(15,8)', 'NOT NULL'],
    ['high', 'DECIMAL(15,8)', 'NOT NULL'],
    ['low', 'DECIMAL(15,8)', 'NOT NULL'],
    ['close', 'DECIMAL(15,8)', 'NOT NULL'],
    ['volume', 'DECIMAL(25,8)', 'NOT NULL'],
    ['close_time', 'BIGINT', 'NOT NULL UNIQUE'],
    ['quote_vol', 'DECIMAL(25,8)', 'NOT NULL'],
    ['num_trades', 'BIGINT', 'NOT NULL'],
    ['buy_base_vol', 'DECIMAL(25,8)', 'NOT NULL'],
    ['buy_quote_vol', 'DECIMAL(25,8)', 'NOT NULL'],
    ['ignored', 'DECIMAL(15,8)', ''],
    ['time_loaded', 'BIGINT']
]


# format data to be written into the table
def adapt_data(data):
    adapted = [[j[0],  # open_time
                Decimal(j[1]),  # open
                Decimal(j[2]),  # high
                Decimal(j[3]),  # low
                Decimal(j[4]),  # close
                Decimal(j[5]),  # volume
                j[6],  # close_time
                Decimal(j[7]),  # quote_vol
                j[8],  # num_trades
                Decimal(j[9]),  # buy_base_vol
                Decimal(j[10]),  # buy_quote_vol
                Decimal(j[11]),  # ignored
                j[12]] for j in data]
    return adapted


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
            with connect(
                    host=self.host,
                    user=self.user,
                    password=self.password
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute('SHOW DATABASES')
                    db_list = cursor.fetchall()
        except Error as e:
            logger.error(f'error while doing list_databases() {e}')
        mysql_system_dbs = [('information_schema',), ('mysql',), ('performance_schema',), ('sys',)]
        return [db[0] for db in db_list if db not in mysql_system_dbs]

    def connect(self):
        try:
            self.conn = connect(host=self.host,
                                user=self.user,
                                password=self.password,
                                database=self.database)
        except Error as err:
            err_message = 'error while connecting to a database'
            logger.error(f'{err_message} {self.database}; {err}')
            raise exceptions.SQLError(err, err_message)
        else:
            self.conn_cursor = self.conn.cursor()
            logger.debug(f'connected to database {self.database}')
            self.connected = True

    def close_connection(self):
        if self.connected:
            self.conn_cursor.close()
            self.conn.close()
            logger.debug(f'closed connection to database {self.database}')
            self.connected = False

    def get_db_and_cursor(self):
        if self.connected:
            return [self.conn, self.conn_cursor]
        else:
            return [None, None]

    def table_create(self, table_name):
        if not self.connected:
            logger.error('cannot create table, not connected to a database, run .connect() first')
            return None
        else:
            try:
                table_struct_string = ',\n'.join([' '.join(i) for i in candle_table_structure])
                create_new_table_query = f"""
                        CREATE TABLE {table_name} (
                        {table_struct_string}
                        )
                        """
                self.conn_cursor.execute(create_new_table_query)
                self.conn.commit()
            except Error as err:
                logger.error(f'could not create table {table_name}, {err}')

    def table_delete(self, table_name):
        if self.table_in_db(table_name):
            try:
                delete_table_query = f'DROP TABLE {table_name}'
                self.conn_cursor.execute(delete_table_query)
                self.conn.commit()
                logger.debug(f'deleted table {table_name}')

            except Error as err:
                err_message = 'unknown error while deleting a table'
                logger.error(f'{err_message} {self.database}; {err}')
                raise exceptions.SQLError(err, err_message)
        else:
            logger.error(f'could not delete table {table_name}, no such table')

    def table_in_db(self, table_name):
        show_req = f'SHOW TABLES LIKE \'{table_name}\''
        tables = None
        try:
            self.conn_cursor.execute(show_req)
            tables = self.conn_cursor.fetchall()
        except Error as err:
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
        except Error as err:
            logger.error(f'failed to list_tables, {err}')
            return None
        return tables

    def count_rows(self, table_name):
        try:
            count_req = f'SELECT COUNT(open_time) FROM {table_name}'
            self.conn_cursor.execute(count_req)
            return self.conn_cursor.fetchone()[0]
        except Error as err:
            logger.error(f'failed to count_rows, {err}')
            return None

    def add_column(self, table_name, column_name, col_type='', not_null='', unique=''):
        add_req = f"""
                ALTER TABLE {table_name}
                ADD COLUMN {column_name} {col_type} {not_null} {unique}
                """
        add_req = ' '.join(add_req.split())
        try:
            self.conn_cursor.execute(add_req)
        except Error as err:
            err_message = 'failed to insert column'
            logger.error(f'{err_message}, {err}')
            raise exceptions.SQLError(err, err_message)

    def get_start_end(self, table_name):
        # get the timestamp of the first and the last entry in existing table
        first_entry, last_entry = None, None
        if self.count_rows(table_name) < 1:  # if table empty
            logger.error(f'tried to get first and last entry from an empty table {table_name}')
        else:
            try:
                first_req = f'SELECT open_time from {table_name} ORDER BY open_time LIMIT 1'
                self.conn_cursor.execute(first_req)
                first_entry = self.conn_cursor.fetchone()[0]
                last_req = f'SELECT open_time from {table_name} ORDER BY open_time DESC LIMIT 1'
                self.conn_cursor.execute(last_req)
                last_entry = self.conn_cursor.fetchone()[0]
            except Error as err:
                logger.error(f'failed to get first and last entry from table {table_name}, {err}')
        return [first_entry, last_entry]

    def truncate(self, table_name):
        try:
            trunc_req = f'TRUNCATE TABLE {table_name}'
            self.conn_cursor.execute(trunc_req)
            self.conn.commit()
        except Error as err:
            logger.error(f'failed to truncate table {table_name}, {err}')

    def write(self, data, table_name):
        headers_list = [i[0] for i in candle_table_structure][1:]
        headers_string = ', '.join(headers_list)
        helper_string = ' ,'.join(['%s'] * len(headers_list))
        insert_req = f"""
                INSERT INTO {table_name}
                ({headers_string})
                VALUES ( {helper_string})
                """
        # logger.debug(insert_req)
        data_to_write = adapt_data(data)
        try:
            self.conn_cursor.executemany(insert_req, data_to_write)
            self.conn.commit()
        except Error as err:
            err_message = 'error writing data into table'
            logger.error(f'{err_message} {table_name}, {err}')
            raise exceptions.SQLError(err, err_message)


if __name__ == '__main__':
    pass

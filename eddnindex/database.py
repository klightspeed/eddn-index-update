from .config import Config, DatabaseConfig
from typing import Protocol, Union, Sequence, Iterator


class DBCursor(Protocol):
    lastrowid: int

    def execute(self, command: str, parameters: Union[Sequence, None] = None):
        ...

    def executemany(self, command: str, parameters: Sequence[Sequence]):
        ...

    def fetchone(self) -> Sequence:
        ...

    def fetchmany(self, size: int) -> Sequence[Sequence]:
        ...

    def fetchall(self) -> Sequence[Sequence]:
        ...

    def __iter__(self) -> Iterator[Sequence]:
        ...


class DBConnection(object):
    config: DatabaseConfig
    connection_type: str
    streaming_cursor_args: list
    streaming_cursor_kwargs: dict
    prepared_cursor_args: list
    prepared_cursor_kwargs: dict
    paramstyle: str

    def __init__(self, config: Config):
        self.streaming_cursor_args = []
        self.streaming_cursor_kwargs = {}
        self.prepared_cursor_args = []
        self.prepared_cursor_kwargs = {}
        self.connection_type = config.database.ConnectionType

        if config.database.ConnectionType == 'mysql.connector':
            import mysql.connector
            self.conn = mysql.connector.connect(
                    user=config.database.Username,
                    host=config.database.Hostname,
                    password=config.database.Password,
                    database=config.database.DatabaseName
            )
            self.conn.set_charset_collation('utf8')
            self.prepared_cursor_kwargs['prepared'] = True
            self.paramstyle = mysql.connector.paramstyle
        elif config.database.ConnectionType == 'mysqlclient':
            import MySQLdb
            import MySQLdb.cursors
            self.conn = MySQLdb.connect(
                    user=config.database.Username,
                    host=config.database.Hostname,
                    password=config.database.Password,
                    database=config.database.DatabaseName,
                    charset='utf8'
            )
            self.streaming_cursor_args = [MySQLdb.cursors.SSCursor]
            self.prepared_cursor_args = [MySQLdb.cursors.SSCursor]
            self.paramstyle = MySQLdb.paramstyle
        elif config.database.ConnectionType == 'pymysql':
            import pymysql
            import pymysql.cursors
            self.conn = pymysql.connect(
                    user=config.database.Username,
                    host=config.database.Hostname,
                    password=config.database.Password,
                    database=config.database.DatabaseName
            )
            self.streaming_cursor_args = [pymysql.cursors.SSCursor]
            self.prepared_cursor_args = [pymysql.cursors.SSCursor]
            self.paramstyle = pymysql.paramstyle
        elif config.database.ConnectionType == 'psycopg2':
            import psycopg2
            self.conn = psycopg2.connect(
                    user=config.database.Username,
                    host=config.database.Hostname,
                    password=config.database.Password,
                    dbname=config.database.DatabaseName
            )
            self.paramstyle = psycopg2.paramstyle
        elif config.database.ConnectionType == 'pymssql':
            import pymssql
            self.conn = pymssql.connect(
                    user=config.database.Username,
                    host=config.database.Hostname,
                    password=config.database.Password,
                    database=config.database.DatabaseName
            )
            self.paramstyle = pymssql.paramstyle
        else:
            raise ValueError('Invalid connection type {0}'.format(config.database.ConnectionType))

    def cursor(self, prepared: bool = False, streaming: bool = False) -> DBCursor:
        if prepared:
            return self.conn.cursor(*self.prepared_cursor_args, **self.prepared_cursor_kwargs)
        elif streaming:
            return self.conn.cursor(*self.streaming_cursor_args, **self.streaming_cursor_kwargs)
        else:
            return self.conn.cursor()

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()

from .config import Config, DatabaseConfig
from typing import Protocol, Union, Any, Optional, Callable
from collections.abc import Mapping, Iterator, Sequence


class DBCursor(Protocol):
    lastrowid: int
    rowcount: int

    def execute(self,
                command: str,
                parameters: Sequence = None):
        ...

    def executemany(self,
                    command: str,
                    parameters: Sequence[Sequence]):
        ...

    def fetchone(self) -> Sequence:
        ...

    def fetchmany(self,
                  size: int
                  ) -> Sequence[Sequence]:
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
    dialect: str
    conn: Any

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
            self.dialect = 'mysql'
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
            self.dialect = 'mysql'
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
            self.dialect = 'mysql'
        elif config.database.ConnectionType == 'psycopg2':
            import psycopg2
            self.conn = psycopg2.connect(
                    user=config.database.Username,
                    host=config.database.Hostname,
                    password=config.database.Password,
                    dbname=config.database.DatabaseName
            )
            self.paramstyle = psycopg2.paramstyle
            self.dialect = 'pgsql'
        elif config.database.ConnectionType == 'pymssql':
            import pymssql
            self.conn = pymssql.connect(
                    user=config.database.Username,
                    host=config.database.Hostname,
                    password=config.database.Password,
                    database=config.database.DatabaseName
            )
            self.paramstyle = pymssql.paramstyle
            self.dialect = 'mssql'
        elif config.database.ConnectionType == 'sqlite3':
            import sqlite3
            self.conn = sqlite3.connect(
                database=config.database.DatabaseName
            )
            self.paramstyle = sqlite3.paramstyle
            self.dialect = 'sqlite3'
        else:
            raise ValueError('Invalid connection type {0}'.format(
                config.database.ConnectionType
            ))

    def cursor(self,
               prepared: bool = False,
               streaming: bool = False
               ) -> DBCursor:
        if prepared:
            return self.conn.cursor(
                *self.prepared_cursor_args,
                **self.prepared_cursor_kwargs
            )
        elif streaming:
            return self.conn.cursor(
                *self.streaming_cursor_args,
                **self.streaming_cursor_kwargs
            )
        else:
            return self.conn.cursor()

    def execute(self,
                cursor: DBCursor,
                query: Union[str, 'SQLQuery'],
                params: Sequence = None
                ) -> DBCursor:
        if isinstance(query, SQLQuery):
            query_string = query.get_query(self)
        else:
            query_string = query

        cursor.execute(query_string, params)
        return cursor

    def executemany(self,
                    cursor: DBCursor,
                    query: Union[str, 'SQLQuery'],
                    params: Sequence[Sequence]
                    ) -> DBCursor:
        if isinstance(query, SQLQuery):
            query_string = query.get_query(self)
        else:
            query_string = query

        cursor.executemany(query_string, params)
        return cursor

    def execute_identity(self,
                         cursor: DBCursor,
                         query: Union[str, 'SQLQuery'],
                         params: Sequence
                         ) -> int:
        if isinstance(query, SQLQuery):
            query_string = query.get_query(self)
            cursor.execute(query_string, params)
            return query.get_last_row_id(self, cursor)
        else:
            query_string = query
            cursor.execute(query_string, params)
            return cursor.lastrowid

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()


class SQLQuery(object):
    query: str
    param_map: Optional[Callable[
        [DBConnection, Union[Sequence, Mapping, None]],
        Union[Sequence, Mapping, None]]
    ]
    last_row_func: Optional[Callable[[DBConnection, DBCursor], int]]
    dialect_queries: Mapping[str, str]

    def __init__(self,
                 query: str,
                 param_map: Optional[Callable[
                     [DBConnection, Union[Sequence, Mapping, None]],
                     Union[Sequence, Mapping, None]
                 ]] = None,
                 last_row_func: Optional[Callable[
                     [DBConnection, DBCursor], int
                 ]] = None,
                 **kwargs: str
                 ):
        self.query = query
        self.param_map = param_map
        self.dialect_queries = kwargs
        self.last_row_func = last_row_func

    def get_query(self, conn: DBConnection):
        query = (self.dialect_queries.get(f'{conn.dialect}:{conn.paramstyle}')
                 or self.dialect_queries.get(conn.dialect)
                 or self.dialect_queries.get(conn.paramstyle)
                 or self.query)

        if conn.paramstyle == 'qmark':
            query = query.replace('%s', '?')

        return query

    def map_params(self,
                   conn: DBConnection,
                   params: Union[Sequence, Mapping, None]
                   ) -> Union[Sequence, Mapping, None]:
        if self.param_map:
            return self.param_map(conn, params)
        elif (isinstance(params, Mapping)
              and conn.paramstyle in ['format', 'qmark', 'numeric']):
            return tuple(params.values())
        else:
            return params

    def get_last_row_id(self, conn: DBConnection, cursor: DBCursor) -> int:
        if self.last_row_func is not None:
            return self.last_row_func(conn, cursor)
        else:
            return cursor.lastrowid

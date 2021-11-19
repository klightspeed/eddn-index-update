from .config import Config, DatabaseConfig
from typing import Protocol, Sequence, Union, Iterator

class DBCursor(Protocol):
    @property
    def lastrowid(self) -> int:
        pass

    def execute(self, command: str, parameters: Union[Sequence, None] = None):
        pass

    def executemany(self, command: str, parameters: Sequence[Sequence]):
        pass

    def fetchone(self) -> Sequence:
        pass

    def fetchmany(self, size: int) -> Sequence[Sequence]:
        pass

    def fetchall(self) -> Sequence[Sequence]:
        pass

    def __iter__(self) -> Iterator[Sequence]:
        pass

class DBConnectionProto(Protocol):
    def cursor(self) -> DBCursor:
        pass

    def close(self):
        pass

class DBConnection(object):
    config: DatabaseConfig
    conn: DBConnectionProto
    streamingcursorargs: list
    streamingcursorkwargs: dict
    prepcursorargs: list
    prepcursorkwargs: dict
    paramstyle: str

    def __init__(self, config: Config):
        self.config = config.database
        self.streamingcursorargs = []
        self.streamingcursorkwargs = {}
        self.prepcursorargs = []
        self.prepcursorkwargs = {}

        if self.config.ConnectionType == 'mysql.connector':
            import mysql.connector
            self.conn = mysql.connector.connect(user=config.database.Username, host=config.database.Hostname, password=config.database.Password, database=config.database.DatabaseName)
            self.conn.set_charset_collation('utf8')
            self.prepcursorkwargs['prepared']
            self.paramstyle = mysql.connector.paramstyle
        elif self.config.ConnectionType == 'mysqlclient':
            import MySQLdb
            import MySQLdb.cursors
            self.conn = MySQLdb.connect(user=config.database.Username, host=config.database.Hostname, password=config.database.Password, database=config.database.DatabaseName, charset='utf8')
            self.streamingcursorargs = [MySQLdb.cursors.SSCursor]
            self.prepcursorargs = [MySQLdb.cursors.SSCursor]
            self.paramstyle = MySQLdb.paramstyle
        elif self.config.ConnectionType == 'pymysql':
            import pymysql
            import pymysql.cursors
            self.conn = pymysql.connect(user=config.database.Username, host=config.database.Hostname, password=config.database.Password, database=config.database.DatabaseName)
            self.streamingcursorargs = [pymysql.cursors.SSCursor]
            self.prepcursorargs = [pymysql.cursors.SSCursor]
            self.paramstyle = pymysql.paramstyle
        elif self.config.ConnectionType == 'psycopg2':
            import psycopg2
            self.conn = psycopg2.connect(user=config.database.Username, host=config.database.Hostname, password=config.database.Password, dbname=config.database.DatabaseName)
            self.paramstyle = psycopg2.paramstyle
        elif self.config.ConnectionType == 'pymssql':
            import pymssql
            self.conn = pymssql.connect(user=config.database.Username, host=config.database.Hostname, password=config.database.Password, database=config.database.DatabaseName)
            self.paramstyle = pymssql.paramstyle
        else:
            raise ValueError('Invalid connection type {0}'.format(config.conntype))

    def cursor(self, prepared: bool = False, streaming: bool = False) -> DBCursor:
        if prepared:
            return self.conn.cursor(*self.prepcursorargs, **self.prepcursorkwargs)
        elif streaming:
            return self.conn.cursor(*self.streamingcursorargs, **self.streamingcursorkwargs)
        else:
            return self.conn.cursor()

    def close(self):
        self.conn.close()

def makepreparedcursor(conn: DBConnection) -> DBCursor:
    return conn.cursor(prepared = True)

def makestreamingcursor(conn: DBConnection) -> DBCursor:
    return conn.cursor(streaming = True)

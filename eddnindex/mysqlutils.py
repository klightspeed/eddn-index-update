from . import config

from collections.abc import Sequence, Mapping
from collections import Iterable
from typing import Callable, Any, Protocol
from typing_extensions import TypeAlias


DBAPITypeCode: TypeAlias = Any | None
DBAPIColumnDescription: TypeAlias = tuple[str, DBAPITypeCode, int | None, int | None, int | None, int | None, bool | None]


class DBAPICursor(Iterable, Protocol):
    @property
    def description(self) -> Sequence[DBAPIColumnDescription] | None: ...
    @property
    def rowcount(self) -> int: ...

    arraysize: int
    lastrowid: int

    def close(self) -> None: ...
    def execute(self, operation: str, parameters: Sequence[Any] | Mapping[str, Any] = ..., /) -> object: ...
    def executemany(self, operation: str, seq_of_parameters: Sequence[Sequence[Any]], /) -> object: ...
    def fetchone(self) -> Sequence[Any] | None: ...
    def fetchmany(self, size: int = ..., /) -> Sequence[Sequence[Any]]: ...
    def fetchall(self) -> Sequence[Sequence[Any]]: ...

    def setinputsizes(self, sizes: Sequence[DBAPITypeCode | int | None], /) -> object: ...
    def setoutputsize(self, size: int, column: int = ..., /) -> object: ...


class DBAPIConnection(Protocol):
    def close(self) -> object: ...
    def commit(self) -> object: ...
    def cursor(self, *args, **kwargs) -> DBAPICursor: ...


createconnection: Callable[[], DBAPIConnection]
makepreparedcursor: Callable[[DBAPIConnection], DBAPICursor]
makestremingcursor: Callable[[DBAPIConnection], DBAPICursor]


if config.conntype == 'mysql.connector':
    import mysql.connector

    def createconnection() -> DBAPIConnection:
        conn = mysql.connector.connect(user=config.sqluser, host=config.sqlhost, password=config.sqlpass, database=config.sqldb)
        conn.set_charset_collation('utf8')
        return conn

    def makepreparedcursor(conn: DBAPIConnection) -> DBAPICursor:
        return conn.cursor(prepared=True)

    def makestreamingcursor(conn: DBAPIConnection) -> DBAPICursor:
        return conn.cursor()

elif config.conntype == 'mysqlclient':
    import MySQLdb
    import MySQLdb.cursors

    def createconnection() -> DBAPIConnection:
        return MySQLdb.connect(user=config.sqluser, host=config.sqlhost, password=config.sqlpass, database=config.sqldb, charset='utf8')

    def makepreparedcursor(conn: DBAPIConnection) -> DBAPICursor:
        return conn.cursor(MySQLdb.cursors.SSCursor)

    def makestreamingcursor(conn: DBAPIConnection) -> DBAPICursor:
        return conn.cursor(MySQLdb.cursors.SSCursor)

elif config.conntype == 'pymysql':
    import pymysql
    import pymysql.cursors

    def createconnection() -> DBAPIConnection:
        return pymysql.connect(user=config.sqluser, host=config.sqlhost, password=config.sqlpass, database=config.sqldb)

    def makepreparedcursor(conn: DBAPIConnection) -> DBAPICursor:
        return conn.cursor(pymysql.cursors.SSCursor)

    def makestreamingcursor(conn: DBAPIConnection) -> DBAPICursor:
        return conn.cursor(pymysql.cursors.SSCursor)

else:
    def createconnection() -> DBAPIConnection:
        raise ValueError('Invalid connection type {0}'.format(config.conntype))

    def makepreparedcursor(conn: DBAPIConnection) -> DBAPICursor:
        raise ValueError('Invalid connection type {0}'.format(config.conntype))

    def makestreamingcursor(conn: DBAPIConnection) -> DBAPICursor:
        raise ValueError('Invalid connection type {0}'.format(config.conntype))

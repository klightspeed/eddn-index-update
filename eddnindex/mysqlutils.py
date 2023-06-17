from . import config

if config.conntype == 'mysql.connector':
    import mysql.connector

    def createconnection():
        conn = mysql.connector.connect(user=config.sqluser, host=config.sqlhost, password=config.sqlpass, database=config.sqldb)
        conn.set_charset_collation('utf8')
        return conn

    def makepreparedcursor(conn):
        return conn.cursor(prepared=True)

    def makestreamingcursor(conn):
        return conn.cursor()

elif config.conntype == 'mysqlclient':
    import MySQLdb
    import MySQLdb.cursors

    def createconnection():
        return MySQLdb.connect(user=config.sqluser, host=config.sqlhost, password=config.sqlpass, database=config.sqldb, charset='utf8')

    def makepreparedcursor(conn):
        return conn.cursor(MySQLdb.cursors.SSCursor)

    def makestreamingcursor(conn):
        return conn.cursor(MySQLdb.cursors.SSCursor)

elif config.conntype == 'pymysql':
    import pymysql
    import pymysql.cursors

    def createconnection():
        return pymysql.connect(user=config.sqluser, host=config.sqlhost, password=config.sqlpass, database=config.sqldb)

    def makepreparedcursor(conn):
        return conn.cursor(pymysql.cursors.SSCursor)

    def makestreamingcursor(conn):
        return conn.cursor(pymysql.cursors.SSCursor)

else:
    def createconnection():
        raise ValueError('Invalid connection type {0}'.format(config.conntype))

    def makepreparedcursor(conn):
        raise ValueError('Invalid connection type {0}'.format(config.conntype))

    def makestreamingcursor(conn):
        raise ValueError('Invalid connection type {0}'.format(config.conntype))

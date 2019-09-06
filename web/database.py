#!/usr/bin/env python
"""wrapper of pymysql for common operations"""

import pymysql


class Database:
    def __init__(self, user, password, dbname):
        self._conn = pymysql.connect(
            host='localhost',
            user=user,
            password=password,
            db=dbname,
            cursorclass=pymysql.cursors.DictCursor
        )
        self._cursor = self._conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()
        self.connection.close()

    @property
    def connection(self):
        return self._conn

    @property
    def cursor(self):
        return self._cursor

    def commit(self):
        self.connection.commit()

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params or ())

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def query(self, sql, params=None):
        self.cursor.execute(sql, params or ())
        return self.fetchall()
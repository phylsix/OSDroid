#!/usr/bin/env python
import json
from datetime import datetime
from os.path import join, dirname, abspath

import pymysql
import yaml
from .serverside import table_schemas
from .serverside.serverside_table import ServerSideTable

DBPATH = join(dirname(abspath(__file__)), "../models/prediction_history.json")
LABELDBPATH = join(dirname(abspath(__file__)), "../models/label_archives.json")

CONFIG_FILE_PATH = join(dirname(abspath(__file__)), "../config/config.yml")

def convert_time(tsecs, fmt="%Y-%m-%d %H:%M:%S"):
    return datetime.fromtimestamp(tsecs).strftime(fmt)


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


class TableBuilder:
    def __init__(self):
        self._config = yaml.load(open(CONFIG_FILE_PATH).read(), Loader=yaml.FullLoader)['mysql']

    def updatetime(self):
        db = Database(*self._config)
        db.execute('SELECT MAX(timestamp) FROM PredictionHistory')
        return db.fetchone()['MAX(timestamp)'].strftime("%Y-%m-%d %H:%M:%S")

    def running_counts(self):
        db = Database(*self._config)
        db.execute("""SELECT COUNT(DISTINCT name)
                      FROM PredictionHistory
                      WHERE timestamp=(SELECT MAX(timestamp)
                      FROM PredictionHistory)""")
        return db.fetchone()['COUNT(DISTINCT name)']

    def archived_counts(self):
        return self.everything_counts()-self.running_counts()

    def everything_counts(self):
        db = Database(*self._config)
        db.execute('SELECT COUNT(DISTINCT name) FROM PredictionHistory')
        return db.fetchone()['COUNT(DISTINCT name)']

    def _rowinfo(self, workflowname):
        db = Database(*self._config)
        sql = """SELECT good, acdc, resubmit, timestamp
                 FROM PredictionHistory
                 WHERE name=%s
                 ORDER BY timestamp ASC"""
        return db.query(sql, (workflowname,))

    def collect_running(self, request):
        db = Database(*self._config)

        tabledata = []
        sql = """SELECT name, good, acdc, resubmit, timestamp
                 FROM PredictionHistory
                 WHERE timestamp=(SELECT MAX(timestamp)
                 FROM PredictionHistory)"""
        tabledata = db.query(sql)
        for i, entry in enumerate(tabledata):
            entry['id'] = str(i)

        columns = table_schemas.SERVERSIDE_TABLE_COLUMNS['running']

        return ServerSideTable(request, tabledata, columns).output_result()

    def collect_archived(self, request):
        labelsource = json.load(open(LABELDBPATH))
        db = Database(*self._config)

        tabledata = []
        sql = """SELECT name, good, acdc, resubmit, timestamp
                 FROM (
                     SELECT * FROM (
                         SELECT * FROM PredictionHistory
                         WHERE timestamp!=(SELECT MAX(timestamp) FROM PredictionHistory)
                      ) AS Q ORDER BY timestamp DESC) AS T
                 GROUP BY name"""
        tabledata = db.query(sql)
        for i, entry in enumerate(tabledata):
            entry['id'] = str(i)
            entry['label'] = labelsource.get(entry['name'], -1)

        columns = table_schemas.SERVERSIDE_TABLE_COLUMNS['archived']

        return ServerSideTable(request, tabledata, columns).output_result()

    def collect_everything(self, request):
        db = Database(*self._config)

        tabledata = []
        sql = """SELECT name, good, acdc, resubmit, timestamp
                 FROM (
                     SELECT * FROM PredictionHistory
                     ORDER BY timestamp DESC
                 ) AS T GROUP BY name"""
        tabledata = db.query(sql)
        for i, entry in enumerate(tabledata):
            entry['id'] = str(i)

        columns = table_schemas.SERVERSIDE_TABLE_COLUMNS['everything']

        return ServerSideTable(request, tabledata, columns).output_result()

    def get_workflow_history(self, wfname):
        rowinfo = self._rowinfo(wfname)
        for record in rowinfo:
            record['timestamp'] = record['timestamp'].strftime("%Y-%m-%d %H:%M:%S")
        return rowinfo

#!/usr/bin/env python
"""Transitions from json source to MySQL db
"""

from os.path import join, dirname, abspath
import json

import pymysql
from ..monitutils import get_yamlconfig

CONFIG_FILE_PATH = join(dirname(abspath(__file__)), '../config/config.yml')


def getconn():
    config = get_yamlconfig(CONFIG_FILE_PATH)
    username_, password_, dbname_ = config['mysql']
    conn = pymysql.connect(
        host='localhost',
        user=username_,
        password=password_,
        db=dbname_,
    )
    return conn


def truncatetable(tablename):
    conn = getconn()

    conn.cursor().execute("truncate table {}".format(tablename))
    conn.commit()
    conn.close()


def createHistDB():
    conn = getconn()

    # conn.cursor().execute('create database OSDroidDB')
    conn.cursor().execute("""create table if not exists OSDroidDB.PredictionHistory (
        hid BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        good FLOAT,
        acdc FLOAT,
        resubmit FLOAT,
        timestamp TIMESTAMP
    ); """)
    conn.commit()
    conn.close()


def insertHist():

    def fmttime(ts):
        from datetime import datetime
        return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

    conn = getconn()

    data = json.load(open('../models/prediction_history.json'))
    wfdata = data['workflowData']
    recordcounts = [len(wfdata[wf]) for wf in wfdata]
    print("# records to insert into PredictionHistory:", sum(recordcounts))

    cursor = conn.cursor()
    for wf in wfdata:
        values = [
            (
                wf,
                round(record['prediction'][0], 6),
                round(record['prediction'][1], 6),
                round(record['prediction'][2], 6),
                fmttime(record['timestamp'])
            )
            for record in wfdata[wf]
        ]
        sql = "INSERT INTO PredictionHistory (name, good, acdc, resubmit, timestamp) VALUES (%s, %s, %s, %s, %s);"
        cursor.executemany(sql, values)
        conn.commit()

    sql = 'SELECT COUNT(hid) FROM PredictionHistory'
    cursor.execute(sql)
    print("# records inserted into PredictionHistory:", cursor.fetchone())
    conn.close()


def createLabelDB():
    conn = getconn()

    # conn.cursor().execute('create database OSDroidDB')
    conn.cursor().execute("""\
        CREATE TABLE IF NOT EXISTS OSDroidDB.LabelArchive (
            name VARCHAR(255) NOT NULL PRIMARY KEY,
            label INT
        );""")
    conn.commit()
    conn.close()


def insertLabel():

    conn = getconn()

    data = json.load(open('../models/label_archives.json'))
    values = list(data.items())
    print("# records to insert into LabelArchive:", len(values))

    sql = "INSERT INTO LabelArchive (name, label) VALUES (%s, %s);"
    conn.cursor().executemany(sql, values)
    conn.commit()

    sql = "SELECT COUNT(name) FROM LabelArchive"
    with conn.cursor() as cursor:
        cursor.execute(sql)
        print("# records inserted into LabelArchive", cursor.fetchone())
    conn.close()



if __name__ == "__main__":

    truncatetable('PredictionHistory')
    insertHist()

    createLabelDB()
    insertLabel()

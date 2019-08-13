#!/usr/bin/env python

import gzip
import json
import os
from datetime import datetime

import pymysql
import cx_Oracle
import yaml
from workflowwrapper import Workflow

# -----------------------------------------------------------------------------

def save_json(json_obj, filename='tmp', gzipped=False):
    """
    save json object to a local formatted text file, for debug

    :param dict json_obj: the json object
    :param str filename: the base name of the file to be saved
    :param bool gzipped: if gzip output document, default is False
    :returns: full filename

    :rtype: str
    """

    fn = "{}.json".format(filename)
    msg = json.dumps(
        json_obj, sort_keys=True, indent=4, separators=(',', ': '))

    if gzipped:
        fn += '.gz'
        with gzip.open(fn, 'wb') as f:
            f.write(msg.encode())
    else:
        with open(fn, 'w') as f:
            f.write(msg)

    return fn

# -----------------------------------------------------------------------------

def get_yamlconfig(configPath):
    '''
    get a dict of config file (YAML) pointed by configPath.

    :param str configPath: path of config file
    :returns: dict of config

    :rtype: dict
    '''

    if not os.path.isfile(configPath):
        return {}

    try:
        return yaml.load(open(configPath).read(), Loader=yaml.FullLoader)
    except:
        return {}


# -----------------------------------------------------------------------------

def get_workflowlist_from_db(config, queryCmd):
    '''
    get a list of workflows from oracle db from a config dictionary which has a ``oracle`` key.

    :param dict config: config dictionary
    :param str queryCmd: SQL query command
    :returns: list of workflow names that are LIKE running

    :rtype: list
    '''

    if 'oracle' not in config:
        return []

    oracle_db_conn = cx_Oracle.connect(*config['oracle'])  # pylint:disable=c-extension-no-member
    oracle_cursor = oracle_db_conn.cursor()
    oracle_cursor.execute(queryCmd)
    wkfs = [row for row, in oracle_cursor]
    oracle_db_conn.close()

    return wkfs

# -----------------------------------------------------------------------------

def get_workflow_from_db(configPath, queryCmd):
    '''
    get a list of :py:class:`Workflow` objects by parsing the oracle db
    indicated in ``config.yml`` pointed by configpath.

    :param str configPath: path of config file
    :param str queryCmd: SQL query command
    :returns: list of :py:class:`Workflow`

    :rtype: list
    '''

    wf_list = []

    config = get_yamlconfig(configPath)
    if not config:
        return wf_list

    wfs = get_workflowlist_from_db(config, queryCmd)
    if wfs:
        wf_list = [Workflow(wf) for wf in wfs]

    return wf_list

# -----------------------------------------------------------------------------

def create_prediction_history_db(config):

    username_, password_, dbname_ = config['mysql']
    conn = pymysql.connect(host='localhost',
                           user=username_,
                           password=password_,
                           db=dbname_)
    try:
        with conn.cursor() as cursor:
            sql = """\
                create table if not exists OSDroidDB.PredictionHistory (
                    hid BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    good FLOAT,
                    acdc FLOAT,
                    resubmit FLOAT,
                    timestamp TIMESTAMP
                ); """
            cursor.execute(sql)

        conn.commit()
    finally:
        conn.close()

# -----------------------------------------------------------------------------

def create_label_archive_db(config):

    username_, password_, dbname_ = config['mysql']
    conn = pymysql.connect(host='localhost',
                           user=username_,
                           password=password_,
                           db=dbname_)
    try:
        with conn.cursor() as cursor:
            sql = """\
                create table if not exists OSDroidDB.LabelArchive (
                    name VARCHAR(255) NOT NULL PRIMARY KEY,
                    lable INT
                ); """
            cursor.execute(sql)

        conn.commit()
    finally:
        conn.close()

# -----------------------------------------------------------------------------

def create_doc_archive_db(config):

    username_, password_, dbname_ = config['mysql']
    conn = pymysql.connect(host='localhost',
                           user=username_,
                           password=password_,
                           db=dbname_)
    try:
        with conn.cursor() as cursor:
            sql = """\
                create table if not exists OSDroidDB.DocsOneMonthArchive (
                    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    document LONGTEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );"""
            cursor.execute(sql)

        conn.commit()
        print("Successfully created table OSDroidDB.DocsOneMonthArchive !")

        with conn.cursor() as cursor:
            sql = """\
                CREATE EVENT OSDroidDB.DocsCleanOneMonth
                ON SCHEDULE EVERY 24 HOUR
                DO
                DELETE FROM OSDroidDB.DocsOneMonthArchive
                WHERE TIMESTAMPDIFF(DAY, timestamp, NOW()) > 30;"""
            cursor.execute(sql)

        conn.commit()
        print("Successfully created event OSDroidDB.DocsCleanOneMonth !")

    finally:
        conn.close()

# -----------------------------------------------------------------------------

def update_prediction_history_db(config, values):

    username_, password_, dbname_ = config['mysql']
    conn = pymysql.connect(host='localhost',
                           user=username_,
                           password=password_,
                           db=dbname_)
    if not isinstance(values, list):
        values = [values,]
    try:
        with conn.cursor() as cursor:
            sql = "INSERT INTO PredictionHistory (name, good, acdc, resubmit, timestamp) VALUES (%s, %s, %s, %s, %s);"
            cursor.executemany(sql, values)

        conn.commit()
    finally:
        conn.close()

# -----------------------------------------------------------------------------

def update_label_archive_db(config, values):

    username_, password_, dbname_ = config['mysql']
    conn = pymysql.connect(host='localhost',
                           user=username_,
                           password=password_,
                           db=dbname_)
    if not isinstance(values, list):
        values = [values,]
    try:
        with conn.cursor() as cursor:
            sql = "REPLACE INTO LabelArchive (name, label) VALUES (%s, %s);"
            cursor.executemany(sql, values)

        conn.commit()
    finally:
        conn.close()

# -----------------------------------------------------------------------------

def update_doc_archive_db(config, value):

    username_, password_, dbname_ = config['mysql']
    conn = pymysql.connect(host='localhost',
                           user=username_,
                           password=password_,
                           db=dbname_)
    try:
        with conn.cursor() as cursor:
            sql = """INSERT INTO OSDroidDB.DocsOneMonthArchive (document) VALUES (%s);"""
            cursor.execute(sql, value)
        conn.commit()
    finally:
        conn.close()

# -----------------------------------------------------------------------------

def get_labeled_workflows(config):

    username_, password_, dbname_ = config['mysql']
    conn = pymysql.connect(host='localhost',
                           user=username_,
                           password=password_,
                           db=dbname_)
    result = []
    try:
        with conn.cursor() as cursor:
            sql = "SELECT name FROM LabelArchive;"
            cursor.execute(sql)
            result = [x[0] for x in cursor.fetchall()]
    finally:
        conn.close()

    return result

# -----------------------------------------------------------------------------

def fmttime(ts, fmt='%Y-%m-%d %H:%M:%S'):
    return datetime.fromtimestamp(ts).strftime(fmt)

# -----------------------------------------------------------------------------

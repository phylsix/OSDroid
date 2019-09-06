#!/usr/bin/env python
"""dump table records from `LabelArchive` for training.
"""
from os.path import abspath, dirname, exists, join
import json

import pymysql
import yaml


CONFIG_FILE_PATH = join(dirname(abspath(__file__)), '../config/config.yml')


def getconn():
    config = yaml.load(open(CONFIG_FILE_PATH).read(), Loader=yaml.FullLoader)
    username_, password_, dbname_ = config['mysql']
    conn = pymysql.connect(
        host='localhost',
        user=username_,
        password=password_,
        db=dbname_,
    )
    return conn

def dump(sql):
    conn = getconn()
    with conn.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchall()
        return result

def main():

    filename = '../models/labelarchivedump.json'
    raw = dump("SELECT * FROM LabelArchive WHERE label!=-1;")
    print("dump {} workflows with known labels.".format(len(raw)))
    json.dump(dict(raw), open(filename, 'w'))

if __name__ == "__main__":
    main()
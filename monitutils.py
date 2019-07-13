#!/usr/bin/env python

import os
import json
import gzip
import yaml
import cx_Oracle

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

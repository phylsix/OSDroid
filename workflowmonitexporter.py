#!/usr/bin/env python

import os
import sys
import time
import sqlite3
import logging
import random
import concurrent.futures
import logging.config
from os.path import join, dirname, abspath

import yaml
from CMSMonitoring.StompAMQ import StompAMQ
from monitutils import get_yamlconfig, get_workflow_from_db
from workflowcollector import populate_error_for_workflow


CRED_FILE_PATH = join(dirname(abspath(__file__)), 'config/credential.yml')
CONFIG_FILE_PATH = join(dirname(abspath(__file__)), 'config/config.yml')
LOGGING_CONFIG = join(dirname(abspath(__file__)), 'config/configLogging.yml')

logger = logging.getLogger("workflowmonitLogger")
rootlogger = logging.getLogger()

class No502WarningFilter(logging.Filter):
    def filter(self, record):
        return 'STATUS: 502' not in record.getMessage()

rootlogger.addFilter(No502WarningFilter())


# -----------------------------------------------------------------------------

def do_work(item):
    """Query, build and return the error doc.

    :param tuple item: (``Workflow``, minFailureRate, configPath)
    :returns: error doc
    :rtype: dict
    """

    wf, minFailureRate, configPath = item

    # database path and insertion command
    dbPath = get_yamlconfig(configPath).get(
        'workflow_status_db',
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'workflow_status.sqlite'))
    DB_UPDATE_CMD = """INSERT OR REPLACE INTO workflowStatuses VALUES (?,?,?)"""

    res = {}

    try:
        time.sleep(random.random()*0.1)
        failurerate = wf.get_failure_rate()
        toUpdate = (wf.name, wf.get_reqdetail().get(wf.name, {}).get(
            'RequestStatus', ''), failurerate)
        if any(toUpdate[:-1]):
            conn = sqlite3.connect(dbPath)
            with conn:
                c = conn.cursor()
                c.execute(DB_UPDATE_CMD, toUpdate)

        if failurerate > minFailureRate:
            res = populate_error_for_workflow(wf)
    except Exception as e:
        logger.exception("workflow<{}> except when do_work!\nMSG: {}".format(
            wf.name, str(e)))
        pass

    return res


# -----------------------------------------------------------------------------

def getCompletedWorkflowsFromDb(configPath):
    """
    Get completed workflow list from local status db (setup to avoid unnecessary caching)

    Workflows whose status ends with *archived* are removed from further caching.

    :param str configPath: location of config file
    :returns: list of workflow (str)
    :rtype: list
    """

    config = get_yamlconfig(configPath)
    if not config:
        sys.exit('Config file: {} not exist, exiting..'.format(configPath))
    dbPath = config.get(
        'workflow_status_db',
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'workflow_status.sqlite'))

    DB_CREATE_CMD = """CREATE TABLE IF NOT EXISTS workflowStatuses (
        name TEXT PRIMARY KEY,
        status TEXT,
        failurerate REAL
    );"""
    DB_QUERY_CMD = """SELECT * FROM workflowStatuses WHERE status LIKE '%archived'"""

    res = []
    conn = sqlite3.connect(dbPath)
    with conn:
        c = conn.cursor()
        c.execute(DB_CREATE_CMD)
        for row in c.execute(DB_QUERY_CMD):
            res.append(row[0])

    return res

# -----------------------------------------------------------------------------

def updateWorkflowStatusToDb(configPath, wcErrorInfos):
    """
    update workflow status to local status db, with the information from ``wcErrorInfos``.

    :param str configPath: location of config file
    :param list wcErrorInfos: list of dicts returned by :py:func:`buildDoc`
    :returns: True
    """

    config = get_yamlconfig(configPath)
    if not config:
        sys.exit('Config path: {} not exist, exiting..'.format(configPath))
    dbPath = config.get(
        'workflow_status_db',
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'workflow_status.sqlite'))

    DB_UPDATE_CMD = """INSERT OR REPLACE INTO workflowStatuses VALUES (?,?,?)"""

    toUpdate = []
    for e in wcErrorInfos:
        entry = (e.get('name', ''), e.get('status', ''),
                 e.get('failureRate', 0.))
        if not all(entry[:-1]):
            continue
        toUpdate.append(entry)

    conn = sqlite3.connect(dbPath)
    with conn:
        c = conn.cursor()
        c.executemany(DB_UPDATE_CMD, toUpdate)

    return True

# -----------------------------------------------------------------------------

def prepareWorkflows(configpath, minfailurerate=0., test=False, batchsize=400):
    """
    extract workflows from unified db, filter out those need to query,
    stratified with batchsize.

    :param str configpath: path to config file
    :param float minfailurerate: input to pack for jobs
    :param bool test: for debug
    "param int batchsize: number of workflows per batch
    :returns: list of list of (:py:class:`Workflow`, `minfailurerate`, `configpath`),
     grouped per `batchsize`.
    :rtype: list
    """

    DB_QUERY_CMD = "SELECT NAME FROM CMS_UNIFIED_ADMIN.WORKFLOW WHERE WM_STATUS LIKE 'running%'"

    _wkfs = []
    try:
        _wkfs = get_workflow_from_db(configpath, DB_QUERY_CMD) # list of `Workflow`
    except Exception as e:
        logger.error("Fail to get running workflows from UNIFIED DB!\nMsg: {}".format(str(e)))
        raise
    msg = 'Number of workflows fetched from db: {}'.format(len(_wkfs))
    logger.info(msg)
    if test: _wkfs = _wkfs[-10:]

    completedWfs = getCompletedWorkflowsFromDb(configpath)
    wkfs = [w for w in _wkfs if w.name not in completedWfs]

    msg = 'Number of workflows to query: {}'.format(len(wkfs))
    logger.info(msg)

    wkfs = [(w, minfailurerate, configpath) for w in wkfs]

    # slice them according to batch size
    res = [wkfs[x:x + batchsize] for x in range(0, len(wkfs), batchsize)]
    msg = 'Divided into {0} batches with batchsize {1}.'.format(len(res), batchsize)
    logger.info(msg)
    return res

# -----------------------------------------------------------------------------

def buildDoc(source, doconcurrent=True, timeout=300):
    """
    Given a list of workflow packs, returns a list of documents (each for one workflow)

    :param list source: a list of workflow packs (tuple)
    :param bool doconcurrent: default True. If True, concurrently execute jobs
    :param float timeout: default 300. timeout limit/seconds for each job when launching jobs parallelly
    :returns: list of documents
    :rtype: list
    """

    results = list()

    startTime = time.time()

    if doconcurrent:
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(source)) as executor:
            futures = {executor.submit(do_work, item): item for item in source}
            for future in concurrent.futures.as_completed(futures, timeout=timeout):
                wfname = futures[future][0].name
                try:
                    res = future.result()
                    if res: results.append(res)
                except Exception as e:
                    print("*** Exception occured in buildDoc ***")
                    print("Workflow:", wfname)
                    print("Msg:", str(e))
    else:
        for item in source:
            _starttime = time.time()
            results.append(do_work(item))
            logger.info("--> took {0}s".format(time.time()-_starttime))

    elapsedTime = time.time() - startTime
    msg = '---> took {}s'.format(elapsedTime)
    logger.info(msg)

    return results

# -----------------------------------------------------------------------------

def sendDoc(cred, docs):
    """
    Given a credential dict and documents to send, make notification.

    :param dict cred: credential required by StompAMQ
    :param list docs: documents to send
    :returns: None
    """

    if not docs:
        logger.info("No document going to be set to AMQ.")
        return []

    try:
        amq = StompAMQ(
            username=None,
            password=None,
            producer=cred['producer'],
            topic=cred['topic'],
            validation_schema=None,
            host_and_ports=[(cred['hostport']['host'],
                             cred['hostport']['port'])],
            logger=logger,
            cert=cred['cert'],
            key=cred['key'])

        doctype = 'workflowmonit_{}'.format(cred['producer'])
        notifications = [
            amq.make_notification(payload=doc, docType=doctype)[0]
            for doc in docs
        ]
        failures = amq.send(notifications)

        logger.info("{}/{} docs successfully sent to AMQ.".format(
            (len(notifications) - len(failures)), len(notifications)))
        return failures

    except Exception as e:
        logger.exception("Failed to send data to StompAMQ. Error: {}".format(
            str(e)))
        raise

# -----------------------------------------------------------------------------


def test():
    with open(LOGGING_CONFIG, 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)

    cred = get_yamlconfig(CRED_FILE_PATH)
    wfpacks = prepareWorkflows(CONFIG_FILE_PATH, test=False)

    # test only the first batch
    firstbatch = wfpacks[0]
    docs = buildDoc(firstbatch, doconcurrent=True)
    updateWorkflowStatusToDb(CONFIG_FILE_PATH, docs)
    logger.info('Number of updated workflows: {}'.format(len(docs)))


    if docs:
        print('Number of docs: ', len(docs))
        if len(str(docs))>500:
            print('[content]', str(docs)[:100], '...', str(docs)[-100:])
        else:
            print('[content]', docs)

    else:
        print("docs empty!!")


if __name__ == "__main__":
    test()

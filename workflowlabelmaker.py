#!/usr/bin/env python
"""
After a workflow enters *-archived status, we can make labels for them such
that comparisons with previous predictions can be made.
For now, the strategy is to query workflow action history from cmsprodmon page
by the its prepid.
Note: because ``get_json`` is used to get workflow prepid, environment variable
`X509_USER_PROXY` must point to a valid proxy.

single workflow name                        ==> Good        | 0
multiple workflow name:
        has 'ACDC' in one of workflow names ==> ACDC-ed     | 1
        no 'ACDC' in any of workflow anmes  ==> Resubmitted | 2
"""

import re
import os
import json
import yaml
import logging
import logging.config
from os.path import join, dirname, abspath
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests_html as rh
from cmstoolbox.webtools import get_json

LOGGING_CONFIG = join(dirname(abspath(__file__)), 'config/configLogging.yml')
DBPATH = join(dirname(abspath(__file__)), "models/label_archives.json")
CMSPRODMONPAGE = 'https://dmytro.web.cern.ch/dmytro/cmsprodmon/workflows.php?prep_id={}'
DIVPATTERN = re.compile(r'<div>([^<]*)</div>')

logger = logging.getLogger("workflowmonitLogger")
rootlogger = logging.getLogger()

class No502WarningFilter(logging.Filter):
    def filter(self, record):
        return 'STATUS: 502' not in record.getMessage()

rootlogger.addFilter(No502WarningFilter())


def get_workflow_param(wfname):

    try:
        result = get_json(
            'cmsweb.cern.ch',
            '/reqmgr2/data/request',
            params={'name': wfname},
            use_https=True,
            use_cert=True
        )
        for params in result['result']:
            for key, item in params.items():
                if key == wfname:
                    return item

    except Exception as e:
        logger.error('Failed to get workflow parameters from reqmgr. workflow: {0}, Msg: {1}'.format(wfname, str(e)) )


def get_action_history(wfname):

    param = get_workflow_param(wfname)
    prepid = str(param.get('PrepID', ''))
    rqstatus = str(param.get('RequestStatus', ''))

    if not prepid or not rqstatus:
        return ''
    if not ('completed' in rqstatus or 'archived' in rqstatus):
        return ''

    queryurl = CMSPRODMONPAGE.format(prepid)
    sess = rh.HTMLSession()
    r = sess.get(queryurl)
    result = re.findall(DIVPATTERN, r.html.find('script')[-1].text)

    return result


def action_histories(wfnames):
    """query histories for a list of workflows

    :param list wfnames: a list of workflow names
    :returns: a dictionary with workflow name as key, associated list of workflow
                names as value
    :rtype: dict
    """
    if not wfnames: return {}

    with ThreadPoolExecutor(max_workers=min(500, len(wfnames))) as executor:
        futures = {executor.submit(get_action_history, wf): wf for wf in wfnames}
        histories = dict()
        for future in as_completed(futures):
            wf = futures[future]
            try:
                histories[wf] = future.result()
            except Exception as e:
                print(str(e))
                print(f"Fail to get history for {wf}")
        return histories


def create_label(wf, wfnames):
    """From a list of workflow names (``wfnames``) assocaited with the same prepid, infer the
    category(label) for the mother workflow (``wf``)

    :param str wf: mother workflow
    :param list wfnames: list of workflow names
    :returns: integer code as label: 0: good, 1: ACDC-ed, 2: Resubmitted, -1: unknown
    :rtype: int
    """

    res = -1
    if not wfnames: return res

    if len(wfnames) == 1 and wfnames[0] == wf:
        res = 0
    if len(wfnames) > 1:
        hasACDC = any(map(lambda name: 'ACDC' in name, wfnames))
        if hasACDC:
            res = 1
        else:
            res = 2

    return res


def label_workflows(wfnames):
    """make labels for a list of workflows

    :param list wfnames: workflow names
    :returns: labels keyed by workflow name
    :rtype: dict
    """

    wflabelmap = {}
    if not wfnames: return wflabelmap

    actionHistories = action_histories(wfnames)
    for wf in actionHistories:
        wflabelmap[wf] = create_label(wf, actionHistories[wf])

    return wflabelmap


def updateLabelArchives(wfnames, dbpath=DBPATH):
    """Given a list of workflownames, make labels for those that has not been
    labelled before, and update db

    :param list wfnames: list of workflow names
    :param str dbpath: path of archive file
    """

    res = {}
    if os.path.exists(dbpath):
        res.update(json.load(open(dbpath)))

    workflowstoquery = [w for w in wfnames if w not in list(res)]
    logger.info("Making labels for {} workflows...".format(len(workflowstoquery)))
    res.update(label_workflows(workflowstoquery))

    with open(dbpath, 'w') as outf:
        outf.write(json.dumps(res))


def test():

    with open(LOGGING_CONFIG, 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)

    # get archived workflows from `/models/prediction_history.json`
    db = json.load(open('models/prediction_history.json'))
    wfupdated_ = db["updatedWorkflows"]
    wfall_ = list(db["workflowData"].keys())
    wfarchived_ = [w for w in wfall_ if w not in wfupdated_]

    import time
    timestart = time.time()
    updateLabelArchives(wfarchived_)
    print("-----> took ", time.time() - timestart, 's')


if __name__ == "__main__":

    test()
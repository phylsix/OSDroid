#!/usr/bin/env python
"""Given a workflow, build a error doc
"""

import re
from collections import defaultdict
from workflowwrapper import Workflow


# -----------------------------------------------------------------------------

def cleanup_shortlog(desc):
    """
    clean up given string by:

    1. remove any HTML tag ``<>``
    2. remove square brackets label ``[]``
    3. remove char ``\\``
    4. replace successive whitespace with a single one
    5. remove single quote/double quote

    :param str desc: a description string
    :returns: a cleaned description string

    :rtype: str
    """

    cleaned = re.compile(r'<.*?>').sub('', desc)
    cleaned = re.compile(r'\[.*?\]').sub('', cleaned)
    cleaned = cleaned.replace('\\', '')
    cleaned = re.compile('\s+').sub(' ', cleaned)
    cleaned = cleaned.replace('"', "'").replace("'", '')

    return cleaned

# -----------------------------------------------------------------------------

def short_errorlog(
        log,
        buzzwords=['error', 'fail', 'exception', 'maxrss', 'timeout'],
        ignorewords=['start', 'begin', 'end', 'above', 'below']):
    r"""
    pruned the lengthy error logs extracted from wmstats to a short message,
    with a logic of combination of ``buzzwords`` and ``ignorewords``.

    - First if ``log`` is short enouggh that does not contain a ``\n``, return it.
    - Else split the log with common delimiters to list, then clean up each entry with :py:func:`cleanup_shortlog`,
        - from begining, if a entry does not contain any word in ``ignorewords`` list, it shall need attention;
        - if a entry contains any word in buzzwords, it shall be buzzed;
        - if a buzzed entry contains less than 3 words, it shall be skipped.( not informative enough)

    - if anything in buzzed list, return a string concatenating all buzzed entries;
    - else if anything in attentioned list, return the first entry;
    - else returns the first entry after clean up only.

    :param str log: length log string from wmstats
    :param list buzzwords: list of words that shall draw attention
    :param list ignorewords: list of words that shall be ignored at any conditions
    :returns: shorted log that shall reflect key information

    :rtype: str
    """

    log = log.strip()
    if '\n' not in log:
        return log

    piecesList = re.split('; |, |:|\*|\n+', log)
    piecesList = [cleanup_shortlog(x) for x in piecesList]

    attentionedPieces = list()
    buzzedPieces = list()

    for piece in piecesList:
        piece = piece.strip()
        raw = piece.lower()

        if any(kw in raw for kw in ignorewords):
            continue
        attentionedPieces.append(piece)

        if any(kw in raw for kw in buzzwords):
            buzzedPieces.append(piece)

    if buzzedPieces:
        buzzedPieces = [x for x in set(buzzedPieces) if len(x.split(' ')) > 2
                        ]  # too short to be informative
    if buzzedPieces:
        return '; '.join(buzzedPieces)

    # should save exceptional error logs to enrich buzzwords
    if attentionedPieces:
        return attentionedPieces[0]
    else:
        return piecesList[0]

# -----------------------------------------------------------------------------

def extract_keywords(
        description,
        buzzwords=[
            'error', 'errors', 'errormsg', 'fail', 'failed', 'failure', 'kill',
            'killed', 'exception'
        ],
        blacklistwords=['start', 'begin', 'end', 'above', 'below'],
        whitelistwords=['timeout', 'maxrss', 'nojobreport']):
    """
    extract keywords from shortened error log,
    with a logic of combination of ``buzzwords``, ``blacklistwords`` and ``whitelistwords``.

    For each word in the ``description``, if it's in ``whitelistwords``, add to return;
    if any word in ``buzzwords`` is a subset of this word, add to return.
    In the end, if any word in ``blacklistwords`` shows up in the to-return list, removes it.

    :param str description: shortened error log
    :param list buzzwords: list of words that shall draw attention
    :param list blacklistwords: list of words that should not be treated as keyword
    :param list whitelistwords: list of words that will always be treated as keyword
    :returns: a set of keywords

    :rtype: set
    """

    kwset = set()

    for word in re.compile('\w+').findall(description):
        word = word.strip()
        raw = word.lower()

        if any(kw in raw for kw in whitelistwords):
            kwset.add(word)

        for kw in buzzwords:
            if kw in raw and (raw not in buzzwords):
                kwset.add(word)

    for kw in blacklistwords:
        kwset.discard(kw)

    return kwset

# -----------------------------------------------------------------------------

def error_logs(workflow):
    """
    Given a :py:class:`Workflow`, builds up a structured entity representing all
    available necessary error information via `Workflo`'s property.::

        {'taskName' :
            {'errorCode' :
                {'siteName' :

                    [
                        {
                            'secondaryErrorCodes' : [],
                            'errorKeywords' : [],
                            'errorChain' : [(typecode, shortLog), ...]
                        },

                        ...
                    ]
                }
            }
        }

    :param workflow: A :py:class:`Workflow` object
    :returns: error info parsed from logs

    :rtype: collections.defaultdict
    """

    error_logs = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: [
    ])))

    wf_jobdetail = workflow.get_jobdetail()
    wf_stepinfo = wf_jobdetail['result'][0].get(workflow.name, {})

    if not wf_stepinfo:
        return error_logs

    for stepname, stepdata in wf_stepinfo.items():
        _taskName = stepname.split('/')[-1]
        # Get the errors from both 'jobfailed' and 'submitfailed' details
        for error, sitedata in [
            (error, sitedata) for status in ['jobfailed', 'submitfailed']
                for error, sitedata in stepdata.get(status, {}).items()
        ]:
            if error == '0':
                continue
            _errorcode = int(error)

            for _sitename, siteinfo in sitedata.items():
                _errorsamples = list()

                for sample in siteinfo['samples']:
                    _timestamp = sample['timestamp']
                    errorcells = [
                        e for cateInfo in sample['errors'].values()
                        for e in cateInfo
                    ]
                    errorcells_unique = list()
                    for ec in errorcells:
                        if ec in errorcells_unique:
                            continue
                        errorcells_unique.append(ec)

                    _secondaryCodes = list()
                    _errorKeywords = list()
                    _errorChainAsDicts = list()

                    for ec in errorcells_unique:

                        type_ = ec['type']
                        code_ = ec['exitCode']
                        shortdetail_ = short_errorlog(ec['details'])

                        if code_ != _errorcode:
                            _secondaryCodes.append(code_)

                        _errorKeywords.extend(
                            list(
                                extract_keywords(' '.join(
                                    [type_, shortdetail_]))))

                        _errorChainAsDicts.append({
                            "errorType": type_,
                            "exitCode": code_,
                            "description": shortdetail_
                        })

                    _errorsamples.append({
                        'secondaryErrorCodes':
                        list(set(_secondaryCodes)),
                        'errorKeywords':
                        list(set(_errorKeywords)),
                        'errorChain':
                        _errorChainAsDicts,
                        'timeStamp':
                        _timestamp
                    })

                error_logs[_taskName][_errorcode][_sitename] = _errorsamples

    return error_logs

# -----------------------------------------------------------------------------

def error_summary(workflow):
    """
    Given a :py:class:`Workflow`,  build a minimal error summary via
    :py:func:`Workflow.get_errors` method.::

        {'taskName':
            {'errors': [
                {
                    'errorCode' : errorCode,
                    'siteName' : siteName,
                    'counts' : counts
                },

                 ...

                ],
            'siteNotReported': []
            }
        }

    :param workflow: A :py:class:`Workflow` object
    :returns: A dict representing mimnial error summary

    :rtype: dict
    """

    error_summary = dict()

    errorInfo = workflow.get_errors()
    if not errorInfo:
        return error_summary

    for fullTaskName, taskErrors in errorInfo.items():
        taskName = fullTaskName.split('/')[-1]
        if not taskName:
            continue

        errorList = list()
        noReportSite = list(taskErrors.get('NotReported', {}).keys())
        for errorCode, siteCnt in taskErrors.items():
            if errorCode == 'NotReported':
                continue

            for siteName, counts in siteCnt.items():
                errorList.append({
                    'errorCode': int(errorCode),
                    'siteName': siteName,
                    'counts': counts
                })

        error_summary[taskName] = {
            'errors': errorList,
            'siteNotReported': noReportSite
        }

    return error_summary

# -----------------------------------------------------------------------------

def populate_error_for_workflow(workflow):
    """
    Given a :py:class:`Workflow`,  build an ultimate error summary with
    :py:func:`error_logs`, :py:func:`error_summary` method and wmstats API.

    :param workflow: A :py:class:`Workflow` object
    :returns: A dict representing all available error info

    :rtype: dict
    """

    if isinstance(workflow, str):
        workflow = Workflow(workflow)
    assert (isinstance(workflow, Workflow))

    workflow_summary = {
        "name": workflow.name,
        "status": None,
        "type": None,
        "failureRate": 0.,
        "totalError": 0,
        "failureKeywords": [],
        "transitions": [],
        "tasks": {}
    }

    workflow_summary['failureRate'] = workflow.get_failure_rate()
    wf_reqdetail = workflow.get_reqdetail()

    wfData = wf_reqdetail.get(workflow.name, {})
    if not wfData:
        return workflow_summary

    # info extracted from wmstats request API
    agentJobInfo = wfData.get('AgentJobInfo', {})
    requestStatus = wfData.get('RequestStatus', None)
    requestType = wfData.get('RequestType', None)
    if not all([agentJobInfo, requestStatus, requestType]):
        return workflow_summary

    requestTransition = wfData.get('RequestTransition', [])

    workflow_summary['status'] = requestStatus
    workflow_summary['type'] = requestType
    workflow_summary['transitions'] = requestTransition

    nfailure = 0
    for agent, agentdata in agentJobInfo.items():
        status = agentdata.get('status', {})
        tasks = agentdata.get('tasks', {})
        if not all([status, tasks]):
            continue

        for ftype, num in status.get('failure', {}).items():
            nfailure += num

        for taskFullName, taskData in tasks.items():
            taskName = taskFullName.split('/')[-1]

            inputTask = None
            if len(taskFullName.split('/')) > 3:
                inputTask = taskFullName.split('/')[-2]

            jobType = taskData.get('jobtype', None)
            taskStatus = taskData.get('status', {})

            taskSiteError = dict()

            if taskStatus and taskStatus.get('failure', {}):
                for site, siteData in taskData.get('sites', {}).items():
                    errCnt = 0
                    errCnts = siteData.get('failure', {})
                    if not errCnts:
                        continue

                    for ftype, cnt in errCnts.items():
                        errCnt += cnt

                    taskSiteError[site] = errCnt

            _task = workflow_summary['tasks'].get(taskName, None)
            if _task:
                if 'jobType' not in _task.keys():
                    _task["jobType"] = jobType
                if 'inputTask' not in _task.keys():
                    _task['inputTask'] = inputTask
                if 'siteErrors' not in _task.keys():
                    _task["siteErrors"] = taskSiteError
                else:
                    for site, errors in taskSiteError.items():
                        if site in _task["siteErrors"].keys():
                            _task["siteErrors"][site] += errors
                        else:
                            _task["siteErrors"][site] = errors

                workflow_summary['tasks'][taskName] = _task
            else:
                workflow_summary['tasks'][taskName] = {
                    "inputTask": inputTask,
                    "jobType": jobType,
                    "siteErrors": taskSiteError,
                    "errors": [],
                    "siteNotReported": []
                }

    # remove tasks that does not have any error
    taskToDel = list()
    for taskname, taskinfo in workflow_summary['tasks'].items():
        if 'siteErrors' in taskinfo and (not taskinfo['siteErrors']):
            taskToDel.append(taskname)
    for taskname in taskToDel:
        workflow_summary['tasks'].pop(taskname, None)

    workflow_summary['totalError'] = nfailure

    if workflow_summary['status'] != 'rejected':

        wf_errorSummary = error_summary(workflow)
        wf_errorLog = error_logs(workflow)

        # add information from errorSummary
        for taskName, taskErrors in wf_errorSummary.items():
            if taskName in workflow_summary['tasks'].keys():
                workflow_summary['tasks'][taskName].update(taskErrors)

        # add information from errorLog
        for taskName, taskErrorLogInfo in wf_errorLog.items():

            if taskName not in workflow_summary['tasks'].keys():
                continue
            for errorCode, siteInfo in taskErrorLogInfo.items():
                for site, info in siteInfo.items():

                    for e in workflow_summary['tasks'][taskName].get(
                            'errors', []):
                        if e.get('siteName', None) != site:
                            continue
                        if e.get('errorCode', None) != errorCode:
                            continue

                        if len(info):
                            e.update(info[0])

        # fill failureKeywords list
        allKeywords = [
            kw for task in workflow_summary['tasks'].values()
            for error in task.get('errors', [])
            for kw in error.get('errorKeywords', [])
        ]
        workflow_summary['failureKeywords'] = list(set(allKeywords))

    # last step, nest in task key(TaskName) as a key-value pair
    tasksAsList = []
    for taskname, taskinfo in workflow_summary['tasks'].items():
        taskinfo.update({"name": taskname})
        taskinfo['siteErrors'] = [
            {
                "site": site,
                "counts": counts
            } for site, counts in taskinfo['siteErrors'].items()
        ]  # convert 'siteErrors' from a dict to a list of dict
        tasksAsList.append(taskinfo)
    workflow_summary['tasks'] = tasksAsList

    return workflow_summary

# -----------------------------------------------------------------------------


def test():

    import time

    wf0 = Workflow(
        'pdmvserv_task_HIG-RunIIAutumn18NanoAOD-00665__v1_T_190112_020711_2622'
    )
    wf1 = Workflow(
        'pdmvserv_task_HIG-RunIIAutumn18NanoAOD-00678__v1_T_190112_061134_937')

    print("<workflow 0>", wf0.name)
    startTime = time.time()
    print("[errdoc]", populate_error_for_workflow(wf0))
    print("----- > took", time.time() - startTime, "s")

    print("<workflow 1>", wf1.name)
    startTime = time.time()
    print("[errdoc]", populate_error_for_workflow(wf1))
    print("----- > took", time.time() - startTime, "s")

    print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

    source = [ Workflow(
        'pdmvserv_task_HIG-RunIIAutumn18NanoAOD-00665__v1_T_190112_020711_2622'
    ), Workflow(
        'pdmvserv_task_HIG-RunIIAutumn18NanoAOD-00678__v1_T_190112_061134_937')]
    results = []
    import concurrent.futures
    startTime = time.time()
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=len(source)) as executor:
        futures = {
            executor.submit(populate_error_for_workflow, item): item
            for item in source
        }
        for future in concurrent.futures.as_completed(futures):
            wfname = futures[future].name
            try:
                res = future.result()
                if res: results.append(res)
            except Exception as e:
                print("*** Exception occured  ***")
                print("Workflow:", wfname)
                print("Msg:", str(e))
    print("[errdocs]", results)
    print("----- > took", time.time() - startTime, "s")


if __name__ == '__main__':
    test()

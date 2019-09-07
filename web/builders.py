#!/usr/bin/env python
import json
import traceback
from collections import defaultdict
from os.path import abspath, dirname, join

import pymysql
import yaml

from .serverside import table_schemas
from .serverside.serverside_table import ServerSideTable
from .database import Database

CONFIG_FILE_PATH = join(dirname(abspath(__file__)), "../config/config.yml")

def convert_time(tsecs, fmt="%Y-%m-%d %H:%M:%S"):
    from datetime import datetime
    return datetime.fromtimestamp(tsecs).strftime(fmt)


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
                 WHERE timestamp=(
                     SELECT MAX(timestamp)
                     FROM PredictionHistory
                 )"""
        tabledata = db.query(sql)
        for i, entry in enumerate(tabledata):
            entry['id'] = str(i)

        columns = table_schemas.SERVERSIDE_TABLE_COLUMNS['running']

        return ServerSideTable(request, tabledata, columns).output_result()

    def collect_running_long(self, request, days=2):
        db = Database(*self._config)

        tabledata = []
        sql = """\
            SELECT B.name, B.good, B.acdc, B.resubmit, B.timestamp
            FROM (
                SELECT *, MAX(timestamp) AS maxts, MIN(timestamp) AS mints
                FROM (
                    SELECT *
                    FROM PredictionHistory
                    ORDER BY timestamp DESC
                ) AS T
                GROUP BY name
                HAVING maxts=(
                    SELECT MAX(timestamp)
                    FROM PredictionHistory
                    )
                    AND TIMESTAMPDIFF(DAY, mints, maxts)>%d
            ) AS B""" % days
        tabledata = db.query(sql)
        for i, entry in enumerate(tabledata):
            entry['id'] = str(i)

        columns = table_schemas.SERVERSIDE_TABLE_COLUMNS['running']

        return ServerSideTable(request, tabledata, columns).output_result()

    def collect_archived(self, request):
        db = Database(*self._config)

        tabledata = []
        sql = """\
            SELECT B.name, B.good, B.acdc, B.resubmit, B.timestamp, COALESCE(LabelArchive.label, -1) AS label
            FROM (
                SELECT *, MAX(timestamp) AS maxts, MIN(timestamp) AS mints
                FROM (
                    SELECT *
                    FROM PredictionHistory
                    ORDER BY timestamp DESC
                ) AS T
                GROUP BY name
                HAVING maxts!=(SELECT MAX(timestamp) FROM PredictionHistory)
            ) AS B
                LEFT JOIN LabelArchive
                ON B.name = LabelArchive.name
        """
        tabledata = db.query(sql)  # list of dictionary
        for i, entry in enumerate(tabledata):
            entry['id'] = str(i)

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


class DocBuilder:
    def __init__(self):
        self._config = yaml.load(open(CONFIG_FILE_PATH).read(), Loader=yaml.FullLoader)['mysql']
        self._lastdoc = None
        self._lasttimestamp = None

    @property
    def lastdoc(self):
        if not self._lastdoc:
            db = Database(*self._config)
            sql = """\
                SELECT document FROM DocsOneMonthArchive
                WHERE timestamp=(
                    SELECT MAX(timestamp)
                    FROM DocsOneMonthArchive
                );"""
            rawdata = db.query(sql)
            self._lastdoc = [json.loads(d['document']) for d in rawdata]
        return self._lastdoc

    @property
    def updatetime(self):
        if not self._lasttimestamp:
            db = Database(*self._config)
            db.execute('SELECT MAX(timestamp) FROM DocsOneMonthArchive;')
            self._lasttimestamp = db.fetchone()['MAX(timestamp)'].strftime("%Y-%m-%d %H:%M:%S")
        return self._lasttimestamp

    def workflow_last_updatetime(self, name):
        db = Database(*self._config)
        sql = """SELECT MAX(timestamp) FROM DocsOneMonthArchive WHERE name=%s;"""
        db.execute(sql, (name,))
        return db.fetchone()['MAX(timestamp)'].strftime("%Y-%m-%d %H:%M:%S")

    def totalerror_per_site(self):
        cnt = defaultdict(int)

        for doc in self.lastdoc:
            for tsk in doc.get('tasks', []):
                for se in tsk.get('siteErrors', []):
                    cnt[se['site']] += se['counts']

        data_ = []
        for k, v in cnt.items():
            data_.append({'site': k, 'errors': v})

        return {'data': data_, 'timestamp': self.updatetime}

    def get_error_report(self, name, timestamp=None):
        db = Database(*self._config)
        if not timestamp:
            sql = "SELECT document FROM DocsOneMonthArchive WHERE name=%s ORDER BY timestamp DESC;"
            db.execute(sql, (name,))
        else:
            sql = "SELECT document FROM DocsOneMonthArchive WHERE name=%s AND timestamp=%s;"
            db.execute(sql, (name, timestamp))
        rawdata = db.fetchone()
        if rawdata:
            return json.loads(rawdata['document'])
        else:
            return None

    def get_history_timestamps(self, name):
        db = Database(*self._config)
        sql="SELECT timestamp FROM DocsOneMonthArchive WHERE name=%s;"
        rawdata = db.query(sql, (name,))
        return [d['timestamp'].strftime("%Y-%m-%d %H:%M:%S") for d in rawdata]


class IssueBuilder:
    def __init__(self):
        self._config = yaml.load(open(CONFIG_FILE_PATH).read(),
                                 Loader=yaml.FullLoader)['mysql']

    def _workflow_running_period(self, workflow):
        db = Database(*self._config)
        sql = "SELECT MAX(timestamp) AS maxts, MIN(timestamp) AS mints FROM PredictionHistory WHERE name=%s"
        result = db.query(sql, (workflow,))[0]
        return result['maxts']-result['mints']

    def _workflow_prediction_fraction(self, workflow, pred=2, dayframe=1):
        """fraction of a certain prediction over a timeframe (in days).
        :param str workflow: workflow name
        :param int pred: prediction label: 0: good, 1: acdc, 2: resubmit
        :param int dayframe: number of days to look
        """
        import numpy as np
        from datetime import timedelta

        db = Database(*self._config, dictcursor=False)
        sql = "SELECT good, acdc, resubmit FROM PredictionHistory WHERE name=%s"
        dataarray = np.array(db.query(sql, (workflow,)))

        sql = "SELECT timestamp FROM PredictionHistory WHERE name=%s"
        tsarray = np.array(db.query(sql, (workflow,))).flatten()
        intimeframe = (tsarray[-1]-timedelta(days=1)) < tsarray

        inframe_preds = dataarray.argmax(axis=1)[intimeframe]
        return (inframe_preds==pred).sum()/inframe_preds.size

    def _get_workflow_report(self, workflow):
        """return the most recent json document of ``workflow``

        :param str workflow: workflow name
        """
        db = Database(*self._config)
        sql = "SELECT document FROM DocsOneMonthArchive WHERE name=%s"
        rawdata = db.query(sql, (workflow,))[-1]['document']
        return json.loads(rawdata)


class WorkflowIssueBuilder(IssueBuilder):
    def __init__(self):
        super().__init__()

    def _running_workflow_names(self):
        """return running workflow names whose predicted probability of *resubmit* > 0.3"""

        db = Database(*self._config)
        sql = "SELECT name FROM PredictionHistory WHERE timestamp=(SELECT MAX(timestamp) FROM PredictionHistory) and resubmit>0.3"
        result = db.query(sql)
        return [d['name'] for d in result]

    def is_workflow_flagged(self, workflow):
        """determine whether a ``workflow`` should be flagged as **workflowIssue**.
        The following conditions need to be met:
        1. running period > 1 day
        2. fraction of prediction of *resubmit* to be rank 1 over past 1 day > 75%
        3. from most recent error report, totalError > 100
        4. from most recent error reprot, failureRate > 50%

        :param str workflow: workflow name
        """
        if self._workflow_running_period(workflow).days < 1:
            return False

        if self._workflow_prediction_fraction(workflow) < 0.75:
            return False

        lastdoc = self._get_workflow_report(workflow)
        if lastdoc['totalError'] < 100:
            return False
        if lastdoc['failureRate'] < 0.5:
            return False

        return True

    def flagged_workflows(self):
        """check all running workflows, collect workflow names flagged by ``is_workflow_flagged``.
        """
        import concurrent.futures

        running_workflownames = self._running_workflow_names()
        flagged_workflownames = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(running_workflownames)) as executor:
            futures = {executor.submit(self.is_workflow_flagged, workflow): workflow for workflow in running_workflownames}
            for future in concurrent.futures.as_completed(futures, timeout=300):
                workflowname = futures[future]
                try:
                    if future.result():
                        flagged_workflownames.append(workflowname)
                except Exception as e:
                    print("Exception in WorkflowIssueBuilder.flagged_workflows".center(80, '*'))
                    print("Workflow:", workflowname)
                    print("Msg:", traceback.format_exc())

        return flagged_workflownames


class SiteIssueBuilder(IssueBuilder):
    def __init__(self):
        super().__init__()

    def _running_workflow_names(self):
        """return running workflow names whose predicted probability of *acdc* > 0.5"""

        db = Database(*self._config)
        sql = "SELECT name FROM PredictionHistory WHERE timestamp=(SELECT MAX(timestamp) FROM PredictionHistory) and acdc>0.5"
        result = db.query(sql)
        return [d['name'] for d in result]

    def _get_workflow_reports(self, workflow):
        db = Database(*self._config)
        sql = "SELECT document FROM DocsOneMonthArchive WHERE name=%s"
        rawdata = db.query(sql, (workflow,))
        return [d['document'] for d in rawdata]

    def _get_two_reports(self, workflow, timespan=4):
        """return two reports to be compared.
        The first one is closest to max(timestamp),
        The second one is closest to max(timestamp)-4h.

        :param str workflow: workflow name
        :param int timespan: hour diff wrt. max(timestamp)
        """
        from datetime import timedelta
        import numpy as np

        db = Database(*self._config, dictcursor=False)
        sql = "SELECT MAX(timestamp) FROM DocsOneMonthArchive"
        ts0 = db.query(sql)[0][0]
        ts1 = ts0 - timedelta(hours=timespan)
        sql = "SELECT timestamp FROM DocsOneMonthArchive WHERE name=%s"
        timestamps = db.query(sql, (workflow,))
        ts1diffarrays = [(x[0]-ts1).total_seconds() for x in timestamps]
        idx = np.abs(np.array(ts1diffarrays)).argmin()

        totalreports = self._get_workflow_reports(workflow)
        # print(len(totalreports), idx, workflow)
        return json.loads(totalreports[-1]), json.loads(totalreports[idx])

    @staticmethod
    def siteerror_from_report(report):
        cnt = defaultdict(int)

        for tsk in report.get('tasks', []):
            for se in tsk.get('siteErrors', []):
                cnt[se['site']] += se['counts']

        return cnt

    @staticmethod
    def siteerror_increase(report_past, report_present):
        res = {}
        cnt_past = SiteIssueBuilder.siteerror_from_report(report_past)
        cnt_present = SiteIssueBuilder.siteerror_from_report(report_present)

        for site in cnt_present:
            res[site] = cnt_present[site] - cnt_past.get(site, 0)
        for site in cnt_past:
            if site not in cnt_present:
                res[site] = -cnt_past[site]

        return res

    def _siteerror_increase_per_workflow(self, workflow):
        if self._workflow_running_period(workflow).seconds < 60*60*4:
            return {}
        report_present, report_past = self._get_two_reports(workflow)
        return SiteIssueBuilder.siteerror_increase(report_past, report_present)

    def flagged_sites(self, threshold=500):
        import concurrent.futures

        running_workflownames = self._running_workflow_names()
        siteerror_increases = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            futures = {executor.submit(self._siteerror_increase_per_workflow, workflow): workflow for workflow in running_workflownames}
            for future in concurrent.futures.as_completed(futures, timeout=300):
                workflowname = futures[future]
                try:
                    res = future.result()
                    if res:
                        siteerror_increases.append(res)
                except Exception as e:
                    print("Exception in SiteIssueBuilder.flagged_sites".center(80, '*'))
                    print("Workflow:", workflowname)
                    print("Msg:", traceback.format_exc())

        siteerror_sum = defaultdict(int)
        for entry in siteerror_increases:
            for site in entry:
                siteerror_sum[site] += entry[site]

        result = []
        for site in siteerror_sum:
            if siteerror_sum[site] > threshold:
                result.append({'site': site, 'errorinc': siteerror_sum[site]})
        return result


if __name__ =="__main__":

    wib = WorkflowIssueBuilder()
    # wf='pdmvserv_task_EXO-RunIISummer15wmLHEGS-05648__v1_T_181004_092049_9366'
    # print(wib.flagged_workflows())

    sib = SiteIssueBuilder()
    # wf='pdmvserv_task_B2G-RunIIFall17wmLHEGS-01648__v1_T_190709_120529_981'
    # print(sib._get_two_reports(wf))
    # print(sib.flagged_sites())
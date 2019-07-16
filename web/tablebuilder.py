#!/usr/bin/env python
import json
from datetime import datetime
from os.path import join, dirname, abspath

from .serverside import table_schemas
from .serverside.serverside_table import ServerSideTable

DBPATH = join(dirname(abspath(__file__)), "../models/prediction_history.json")
LABELDBPATH = join(dirname(abspath(__file__)), "../models/label_archives.json")


def convert_time(tsecs, fmt="%Y-%m-%d %H:%M:%S"):
    return datetime.fromtimestamp(tsecs).strftime(fmt)



class TableBuilder:
    def __init__(self):
        self.db_ = json.load(open(DBPATH))

    def updatetime(self):
        return self.db_["updateTime"]

    def running_counts(self):
        updatedwfs_ = self.db_["updatedWorkflows"]
        return len(updatedwfs_)

    def archived_counts(self):
        wfupdated_ = self.db_["updatedWorkflows"]
        wfall_ = self.db_["workflowData"]
        wfarchived_ = [w for w in wfall_ if w not in wfupdated_]
        return len(wfarchived_)

    def everything_counts(self):
        wfall_ = self.db_["workflowData"]
        return len(wfall_)


    def collect_running(self, request):
        updatedwfs_ = self.db_["updatedWorkflows"]
        workflowdata_ = self.db_["workflowData"]

        tabledata = []
        for i, wf in enumerate(updatedwfs_):
            if wf not in workflowdata_:
                continue

            tableentry = {"id": str(i), "name": wf}
            lastpred = workflowdata_[wf][-1]
            probs = [round(x, 6) for x in lastpred["prediction"]]
            tableentry["good"] = probs[0]
            tableentry["acdc"] = probs[1]
            tableentry["resubmit"] = probs[2]
            tableentry["timestamp"] = convert_time(lastpred["timestamp"])
            tableentry["history"] = [
                dict(timestamp=convert_time(e["timestamp"]), prediction=e["prediction"])
                for e in workflowdata_[wf]
            ]
            tabledata.append(tableentry)

        columns = table_schemas.SERVERSIDE_TABLE_COLUMNS['running']

        return ServerSideTable(request, tabledata, columns).output_result()


    def collect_archived(self, request):
        labelsource = json.load(open(LABELDBPATH))
        wfupdated_ = self.db_["updatedWorkflows"]
        wfall_ = self.db_["workflowData"]
        wfarchived_ = [w for w in wfall_ if w not in wfupdated_]
        workflowdata_ = self.db_["workflowData"]

        tabledata = []
        for i, wf in enumerate(wfarchived_):
            tableentry = {"id": str(i), "name": wf}
            lastpred = workflowdata_[wf][-1]
            probs = [round(x, 6) for x in lastpred["prediction"]]
            tableentry["good"] = probs[0]
            tableentry["acdc"] = probs[1]
            tableentry["resubmit"] = probs[2]
            tableentry["timestamp"] = convert_time(lastpred["timestamp"])
            tableentry["history"] = [
                dict(timestamp=convert_time(e["timestamp"]), prediction=e["prediction"])
                for e in workflowdata_[wf]
            ]
            tableentry['label'] = labelsource.get(wf, -1)

            tabledata.append(tableentry)

        columns = table_schemas.SERVERSIDE_TABLE_COLUMNS['archived']

        return ServerSideTable(request, tabledata, columns).output_result()


    def collect_everything(self, request):
        wfall_ = self.db_["workflowData"]
        workflowdata_ = self.db_["workflowData"]

        tabledata = []
        for i, wf in enumerate(wfall_):
            tableentry = {"id": str(i), "name": wf}
            lastpred = workflowdata_[wf][-1]
            probs = [round(x, 6) for x in lastpred["prediction"]]
            tableentry["good"] = probs[0]
            tableentry["acdc"] = probs[1]
            tableentry["resubmit"] = probs[2]
            tableentry["timestamp"] = convert_time(lastpred["timestamp"])
            tableentry["history"] = [
                dict(timestamp=convert_time(e["timestamp"]), prediction=e["prediction"])
                for e in workflowdata_[wf]
            ]

            tabledata.append(tableentry)

        columns = table_schemas.SERVERSIDE_TABLE_COLUMNS['everything']

        return ServerSideTable(request, tabledata, columns).output_result()
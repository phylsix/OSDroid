from flask import Flask, render_template
from . import app
import json
import time
from datetime import datetime
from os.path import join, abspath, dirname

DBPATH = join(dirname(abspath(__file__)), "../models/prediction_history.json")
TABLEDATAPATH = join(dirname(abspath(__file__)), "static/tabledata_{}.json")


def convert_time(tsecs, fmt="%Y-%m-%d %H:%M:%S"):
    return datetime.fromtimestamp(tsecs).strftime(fmt)


@app.route("/")
def home():
    db = json.load(open(DBPATH))
    updatetime_ = time.ctime(db["updateTime"])
    updatedwfs_ = db["updatedWorkflows"]
    workflowdata_ = db["workflowData"]

    tabledata = {"data": []}
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
        tabledata["data"].append(tableentry)

    with open(TABLEDATAPATH.format("running"), "w") as f:
        f.write(json.dumps(tabledata))

    data = dict(
        updatetime=updatetime_,
        count=len(updatedwfs_),
        jsonname="tabledata_running.json",
    )
    return render_template("home.html", **data)


@app.route("/archived/")
def archived():
    db = json.load(open(DBPATH))
    wfupdated_ = db["updatedWorkflows"]
    wfall_ = list(db["workflowData"].keys())
    wfarchived_ = [w for w in wfall_ if w not in wfupdated_]
    workflowdata_ = db["workflowData"]

    tabledata = {"data": []}
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
        tabledata["data"].append(tableentry)
    with open(TABLEDATAPATH.format("archived"), "w") as f:
        f.write(json.dumps(tabledata))

    return render_template(
        "archived.html", count=len(wfarchived_), jsonname="tabledata_archived.json"
    )


@app.route("/everything/")
def everything():
    db = json.load(open(DBPATH))
    wfall_ = list(db["workflowData"].keys())
    workflowdata_ = db["workflowData"]

    tabledata = {"data": []}
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
        tabledata["data"].append(tableentry)
    with open(TABLEDATAPATH.format("everything"), "w") as f:
        f.write(json.dumps(tabledata))

    return render_template(
        "everything.html", count=len(wfall_), jsonname="tabledata_everything.json"
    )

#!/usr/bin/env python
from flask import Blueprint, jsonify, request
from .tablebuilder import TableBuilder

tables = Blueprint('tables', __name__, url_prefix='/tables')


@tables.route("running_table", methods=['GET'])
def running_table_content():
    return jsonify(TableBuilder().collect_running(request))

@tables.route("archived_table", methods=['GET'])
def archived_table_content():
    return jsonify(TableBuilder().collect_archived(request))

@tables.route("everything_table", methods=['GET'])
def everything_table_content():
    return jsonify(TableBuilder().collect_everything(request))


predhistory = Blueprint('predhistory', __name__, url_prefix='/predhistory')

@predhistory.route('<wfname>', methods=['GET'])
def workflow_history(wfname):
    return jsonify(TableBuilder().get_workflow_history(wfname))
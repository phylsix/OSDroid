#!/usr/bin/env python
from flask import Blueprint, request, render_template, jsonify
from .tablebuilder import TableBuilder, DocBuilder

###############################################################################

main = Blueprint('main', __name__, url_prefix='')

@main.route('/')
def index():
    tb = TableBuilder()
    data_ = {
        "updatetime": tb.updatetime(),
        "count": tb.running_counts(),
        "page": 'running',
    }
    return render_template('home.html', **data_)

@main.route('/running2days')
def running2days():
    return render_template('running2days.html', page='running2days')

@main.route('/running7days')
def running7days():
    return render_template('running7days.html', page='running7days')

@main.route('/running2weeks')
def running2weeks():
    return render_template('running2weeks.html', page='running2weeks')

@main.route('/archived')
def archived():
    tb = TableBuilder()
    data_ = {
        "count": tb.archived_counts(),
        "page": 'archived',
    }
    return render_template('archived.html', **data_)

@main.route('/everything')
def everything():
    tb = TableBuilder()
    data_ = {
        "count": tb.everything_counts(),
        "page": 'everything',
    }
    return render_template('everything.html', **data_)

@main.route('/siteerrors')
def siteerrors():
    docbuilder_ = DocBuilder()
    data_ = {
        "updatetime": docbuilder_.updatetime,
        "page": 'siteerrors',
    }
    return render_template('siteerrors.html', **data_)

@main.route('/errorreport/<wfname>')
def errorreport(wfname):
    docbuilder_ = DocBuilder()
    data_ = {
        "updatetime": docbuilder_.workflow_last_updatetime(wfname),
        "name": wfname,
    }
    return render_template('errorreport.html', **data_)

###############################################################################

tables = Blueprint('tables', __name__, url_prefix='/tables')


@tables.route("running_table", methods=['GET'])
def running_table_content():
    return jsonify(TableBuilder().collect_running(request))

@tables.route("running2days_table", methods=['GET'])
def running2days_table_content():
    return jsonify(TableBuilder().collect_running_long(request))

@tables.route("running7days_table", methods=['GET'])
def running7days_table_content():
    return jsonify(TableBuilder().collect_running_long(request, days=7))

@tables.route("running2weeks_table", methods=['GET'])
def running2weeks_table_content():
    return jsonify(TableBuilder().collect_running_long(request, days=14))

@tables.route("archived_table", methods=['GET'])
def archived_table_content():
    return jsonify(TableBuilder().collect_archived(request))

@tables.route("everything_table", methods=['GET'])
def everything_table_content():
    return jsonify(TableBuilder().collect_everything(request))


###############################################################################

predhistory = Blueprint('predhistory', __name__, url_prefix='/predhistory')

@predhistory.route('<wfname>', methods=['GET'])
def workflow_history(wfname):
    return jsonify(TableBuilder().get_workflow_history(wfname))

###############################################################################

docs = Blueprint('docs', __name__, url_prefix='/docs')

@docs.route('site_errors', methods=['GET'])
def site_errors():
    return jsonify(DocBuilder().totalerror_per_site())

@docs.route('/lastdoc')
def lastdoc():
    return jsonify(DocBuilder().lastdoc)

@docs.route('/errorreport/<wfname>', methods=['GET'])
def workflow_errorreport(wfname):
    report = DocBuilder().get_error_report(wfname)
    response = jsonify(report)
    if report is None:
        response.status_code = 404
    return response
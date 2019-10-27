#!/usr/bin/env python
from os.path import abspath, dirname, join

import yaml
from flask import (Blueprint, flash, jsonify, redirect, render_template,
                   request, url_for)

from .builders import (DocBuilder, SiteIssueBuilder, TableBuilder,
                       WorkflowIssueBuilder)
from .cache import cache
from .forms import (IssueSettingForm, getSiteIssueSettings,
                    getWorkflowIssueSettings)

CONFIG_FILE_PATH = join(dirname(abspath(__file__)), "../config/config.yml")

###############################################################################

main = Blueprint('main', __name__, url_prefix='')

@main.route('/')
@cache.cached()
def index():
    tb = TableBuilder()
    data_ = {
        "updatetime": tb.updatetime(),
        "count": tb.running_counts(),
        "page": 'running',
    }
    return render_template('home.html', **data_)

@main.route('/running2days')
@cache.cached()
def running2days():
    return render_template('running2days.html', page='running2days')

@main.route('/running7days')
@cache.cached()
def running7days():
    return render_template('running7days.html', page='running7days')

@main.route('/running2weeks')
@cache.cached()
def running2weeks():
    return render_template('running2weeks.html', page='running2weeks')

@main.route('/archived')
@cache.cached()
def archived():
    tb = TableBuilder()
    data_ = {
        "count": tb.archived_counts(),
        "page": 'archived',
    }
    return render_template('archived.html', **data_)

@main.route('/everything')
@cache.cached()
def everything():
    tb = TableBuilder()
    data_ = {
        "count": tb.everything_counts(),
        "page": 'everything',
    }
    return render_template('everything.html', **data_)

@main.route('/siteerrors')
@cache.cached()
def siteerrors():
    docbuilder_ = DocBuilder()
    data_ = {
        "updatetime": docbuilder_.updatetime,
        "page": 'siteerrors',
    }
    return render_template('siteerrors.html', **data_)

@main.route('/issuesettings', methods=['GET', 'POST'])
def issuesettings():
    form = IssueSettingForm()

    if form.validate_on_submit():
        settings = dict(
            workflow=dict(
                runningDays=float(form.wf_runningDays.data),
                resubmitProb=float(form.wf_resubmitProb.data),
                resubmitAsTopFrac=float(form.wf_resubmitAsTopFrac.data),
                totalError=int(form.wf_totalError.data),
                failureRate=float(form.wf_failureRate.data),
            ),
            site=dict(
                runningHours=float(form.site_runningHours.data),
                acdcProb=float(form.site_acdcProb.data),
                errorCountInc=int(form.site_errorCountInc.data),
            )
        )
        globalconfig = yaml.load(open(CONFIG_FILE_PATH).read(), Loader=yaml.FullLoader)
        globalconfig['issueSentinel'] = settings
        yaml.dump(globalconfig, open(CONFIG_FILE_PATH, 'w'), default_flow_style=False)
        flash("Successfully submit!")

    sitesettings = getSiteIssueSettings()
    wfsettings = getWorkflowIssueSettings()
    data_ = {
        "page": 'issuesettings',
        "form": form,
        "site_runningHours": sitesettings['runningHours'],
        "site_acdcProb": sitesettings['acdcProb'],
        "site_errorCountInc": sitesettings['errorCountInc'],
        "wf_runningDays": wfsettings['runningDays'],
        "wf_resubmitProb": wfsettings['resubmitProb'],
        "wf_resubmitAsTopFrac": wfsettings['resubmitAsTopFrac'],
        "wf_totalError": wfsettings['totalError'],
        "wf_failureRate": wfsettings['failureRate'],
    }

    return render_template('issuesettings.html', **data_)

@main.route('/errorreport')
@cache.cached()
def errorreport():
    wfname = request.args.get('name', default='', type=str)
    timestamp = request.args.get('timestamp', default='', type=str)
    data_ = {"name": wfname,}
    if timestamp:
        data_["updatetime"] = timestamp
    else:
        docbuilder_ = DocBuilder()
        data_['updatetime'] = docbuilder_.workflow_last_updatetime(wfname)

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
@cache.cached()
def workflow_history(wfname):
    return jsonify(TableBuilder().get_workflow_history(wfname))

###############################################################################

docs = Blueprint('docs', __name__, url_prefix='/docs')

@docs.route('site_errors', methods=['GET'])
@cache.cached()
def site_errors():
    return jsonify(DocBuilder().totalerror_per_site())

@docs.route('/lastdoc')
@cache.cached()
def lastdoc():
    return jsonify(DocBuilder().lastdoc)

@docs.route('/errorreport', methods=['GET'])
@cache.cached()
def workflow_errorreport():
    wfname = request.args.get('name', default='', type=str)
    timestamp = request.args.get('timestamp', default='', type=str)
    report = DocBuilder().get_error_report(wfname, timestamp=timestamp)
    response = jsonify(report)
    if report is None:
        response.status_code = 404
    return response

@docs.route('/timestamps/<wfname>', methods=['GET'])
@cache.cached()
def errorreport_timestamps(wfname):
    return jsonify(DocBuilder().get_history_timestamps(wfname))

###############################################################################

issues = Blueprint('issues', __name__, url_prefix='/issues')

@issues.route('workflow', methods=['GET'])
@cache.cached()
def workflow_issues():
    return jsonify(WorkflowIssueBuilder().flagged_workflows())

@issues.route('site', methods=['GET'])
@cache.cached()
def site_issues():
    return jsonify(SiteIssueBuilder().flagged_sites())

#!/usr/bin/env python
from flask import Blueprint, render_template
from .tablebuilder import TableBuilder


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
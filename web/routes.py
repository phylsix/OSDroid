#!/usr/bin/env python
import time
from flask import Blueprint, render_template
from .tablebuilder import TableBuilder


main = Blueprint('main', __name__, url_prefix='')

@main.route('/')
def index():
    tb = TableBuilder()
    data_ = {
        "updatetime": time.ctime(tb.updatetime()),
        "count": tb.running_counts()
    }
    return render_template('home.html', **data_)

@main.route('/archived')
def archived():
    tb = TableBuilder()
    data_ = {
        "count": tb.archived_counts()
    }
    return render_template('archived.html', **data_)

@main.route('/everything')
def everything():
    tb = TableBuilder()
    data_ = {
        "count": tb.everything_counts()
    }
    return render_template('everything.html', **data_)
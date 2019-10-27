#!/usr/bin/env python
from os.path import abspath, dirname, join

import yaml
from flask_wtf import FlaskForm
from wtforms.fields import (DecimalField, IntegerField, PasswordField,
                            SubmitField)
from wtforms.validators import InputRequired, NumberRange, ValidationError

CONFIG_FILE_PATH = join(dirname(abspath(__file__)), "../config/config.yml")

def getWorkflowIssueSettings():
    settings = dict(
        runningDays=1,
        resubmitProb=0.3,
        resubmitAsTopFrac=0.75,
        totalError=100,
        failureRate=0.5,
    )
    globalconfig = yaml.load(open(CONFIG_FILE_PATH).read(), Loader=yaml.FullLoader)
    fromconfig = globalconfig.get('issueSentinel', {}).get('workflow', {})
    if fromconfig:
        settings.update(fromconfig)
    return settings

def getSiteIssueSettings():
    settings = dict(
        runningHours=4,
        acdcProb=0.5,
        errorCountInc=500,
    )
    globalconfig = yaml.load(open(CONFIG_FILE_PATH).read(), Loader=yaml.FullLoader)
    fromconfig = globalconfig.get('issueSentinel', {}).get('site', {})
    if fromconfig:
        settings.update(fromconfig)
    return settings


def check_pin(form, field):
    globalconfig = yaml.load(open(CONFIG_FILE_PATH).read(), Loader=yaml.FullLoader)
    if field.data != globalconfig['issueSentinelPin']:
        raise ValidationError(message="Wrong pin, input settings will NOT take any effect!")


class IssueSettingForm(FlaskForm):
    wf_runningDays = DecimalField("runningDays", default=getWorkflowIssueSettings()['runningDays'], validators=[NumberRange(min=0.)])
    wf_resubmitProb = DecimalField("resubmitProb", default=getWorkflowIssueSettings()['resubmitProb'], validators=[NumberRange(min=0., max=1.)])
    wf_resubmitAsTopFrac = DecimalField("resubmitAsTopFrac", default=getWorkflowIssueSettings()['resubmitAsTopFrac'], validators=[NumberRange(min=0., max=1.)])
    wf_totalError = IntegerField("totalError", default=getWorkflowIssueSettings()['totalError'], validators=[NumberRange(min=0)])
    wf_failureRate = DecimalField("failureRate", default=getWorkflowIssueSettings()['failureRate'], validators=[NumberRange(min=0., max=1.)])

    site_runningHours = DecimalField("runningHours", default=getSiteIssueSettings()['runningHours'], validators=[NumberRange(min=0.)])
    site_acdcProb = DecimalField("acdcProb", default=getSiteIssueSettings()['acdcProb'], validators=[NumberRange(min=0., max=1.)])
    site_errorCountInc = IntegerField("errorCountInc", default=getSiteIssueSettings()['errorCountInc'], validators=[NumberRange(min=0)])

    pin = PasswordField("pin", validators=[InputRequired(), check_pin])
    submit = SubmitField("submit")

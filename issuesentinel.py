#!/usr/bin/env python
"""check workflow issue and site issue detections,
then send notifications if any.
"""
import json
import smtplib
import urllib.request
from email.mime.text import MIMEText
from os.path import abspath, dirname, isfile, join

import jira
from monitutils import get_yamlconfig

CRED_FILE_PATH = join(dirname(abspath(__file__)), 'config/credential.yml')


class JiraClient:
    def __init__(self):
        self._server = 'https://its.cern.ch/jira'
        cookiefile = get_yamlconfig(CRED_FILE_PATH).get('jiracookie', None)
        if not cookiefile or not isfile(cookiefile):
            raise ValueError("`jiracookie` not existed in credential.yml or file not exist!\nJiraClient cannot be constructed.")
        cookies = {}
        for l in open(cookiefile).readlines():
            _l = l.split()
            if len(_l) < 7:
                continue
            if _l[5] in ['JSESSIONID', 'atlassian.xsrf.token']:
                cookies[_l[5]] = _l[6]
        if not cookies:
            raise ValueError("`jiracookie` file corrupted!")
        self.client = jira.JIRA(self._server, options=dict(cookies=cookies))

    def create_issue(self, **kwargs):
        fields = {
            'project': 'CMSCOMPTNITEST',
            'issuetype': {'name': 'Task'},
        }

        if kwargs.get('label', None):
            fields['labels'] = [kwargs['label']]
        if kwargs.get('assignee', None):
            fields['assignee'] = dict(name=kwargs['assignee'], key=kwargs['assignee'])
        if kwargs.get('summary', None):
            fields['summary'] = kwargs['summary']
        if kwargs.get('description', None):
            fields['description'] = kwargs['description']

        return self.client.create_issue(fields)

def send_email(subject, msg, recipients):
    sender = 'toolsandint-workflowmonitalert@cern.ch'

    contentMsg = MIMEText(msg)
    contentMsg['Subject'] = subject
    contentMsg['From'] = sender
    contentMsg['To'] = ', '.join(recipients)
    s = smtplib.SMTP('localhost')
    s.sendmail(sender, recipients, contentMsg.as_string())
    s.quit()


def main():

    osdroid_addr = 'http://localhost:8020'
    jc = JiraClient()

    with urllib.request.urlopen(url=f'{osdroid_addr}/issues/workflow', timeout=60*8) as url:
        flagged_workflows = json.loads(url.read().decode())
        if flagged_workflows:
            fmt = '* {0} [unified|https://cms-unified.web.cern.ch/cms-unified//report/{0}] [OSDroid|{1}/errorreport?name={0}]'
            desc = '\n'.join([fmt.format(workflow, osdroid_addr) for workflow in flagged_workflows])
            jc.create_issue(label='WorkflowIssue',
                            summary=f'{len(flagged_workflows)} potential workflow issue(s) detected',
                            description=desc)

    with urllib.request.urlopen(url=f'{osdroid_addr}/issues/site', timeout=60*8) as url:
        flagged_sites = json.loads(url.read().decode())
        if flagged_sites:
            fmt = "* {0}: {1}"
            desc = '\n'.join([fmt.format(x['site'], x['errorinc']) for x in flagged_sites])
            jc.create_issue(label='SiteIssue',
                            summary=f'{len(flagged_sites)} potential site issue(s) detected',
                            description=desc)



if __name__ == "__main__":
    main()

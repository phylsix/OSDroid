#!/usr/bin/env python
"""check workflow issue and site issue detections,
then send notifications if any.
"""
import json
import smtplib
import socket
import urllib.request
from datetime import datetime
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

    def search_issue(self, label, identifier):
        assert(label in ['WorkflowIssue', 'SiteIssue'])
        jql = 'project=%s AND labels=%s AND summary~%s'
        return self.client.search_issues(jql % ('CMSCOMPTNITEST', label, identifier))

    def add_comment(self, issuekey, comment):
        return self.client.add_comment(issuekey, comment)



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
    host_addr = f'http://{socket.gethostname()}:8020'
    time_str = '{} {}'.format(datetime.now().isoformat(sep=' ', timespec='seconds'),
                              datetime.now().astimezone().tzname())
    jc = JiraClient()

    with urllib.request.urlopen(url=f'{osdroid_addr}/issues/workflow', timeout=60*8) as url:
        flagged_workflows = json.loads(url.read().decode())
        for workflowinfo in flagged_workflows:
            workflow = workflowinfo.pop('name')
            details = json.dumps(workflowinfo, indent=4)
            issues = jc.search_issue(label='WorkflowIssue', identifier=workflow)
            if issues:
                comment = f'<sentinel> detected on {time_str}\n'
                comment += '---------------------------------\n'
                comment += details
                jc.add_comment(issues[0].key, comment)
            else:
                desc = '\n'.join([
                    f'* [unified|https://cms-unified.web.cern.ch/cms-unified//report/{workflow}]',
                    f'* [OSDroid|{host_addr}/errorreport?name={workflow}]',
                ])
                desc += '\n\n\n'
                desc += details
                jc.create_issue(label='WorkflowIssue',
                                summary=f'<Workflow> - {workflow} needs attention',
                                description=desc)

    with urllib.request.urlopen(url=f'{osdroid_addr}/issues/site', timeout=60*8) as url:
        flagged_sites = json.loads(url.read().decode())
        for siteinfo in flagged_sites:
            site = siteinfo.pop('site')
            details = json.dumps(siteinfo, indent=4)
            desc = 'site: {}, error increased: {}, detected on {}'.format(site,
                                                                          siteinfo['errorinc'],
                                                                          time_str)
            issues = jc.search_issue(label='SiteIssue', identifier=site)
            if issues:
                comment = f'<sentinel> {desc}\n'
                comment += '--------------------------------\n'
                comment += details
                jc.add_comment(issues[0].key, comment)
            else:
                desc_ = desc + '\n'
                desc_ += '--------------------------------\n'
                desc_ += details
                jc.create_issue(label='SiteIssue',
                                summary='<Site> - {} needs attention'.format(site),
                                description=desc_)


if __name__ == "__main__":
    main()

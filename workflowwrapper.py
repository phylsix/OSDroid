#!/usr/bin/env python
"""simple wrapper around workflow, only has functions needed for monitoring
"""

from cmstoolbox.webtools import get_json


class Workflow:
    def __init__(self, wfname, url="cmsweb.cern.ch"):
        self.name_ = wfname
        self.url_ = url
        self.reqdetail_ = None
        self.jobdetail_ = None

    @property
    def name(self):
        return self.name_

    def get_jobdetail(self):
        if self.jobdetail_ is None:
            self.jobdetail_ = get_json(
                self.url_,
                f"/wmstatsserver/data/jobdetail/{self.name_}",
                use_cert=True)
        return self.jobdetail_

    def get_reqdetail(self):
        if self.reqdetail_ is None:
            reqDetail = {self.name_: dict()}
            raw = get_json(
                self.url_,
                f'/wmstatsserver/data/request/{self.name_}',
                use_cert=True)
            result = raw.get('result', None)
            if result is None:
                return reqDetail

            reqDetail[self.name_] = result[0].get(self.name_, {})
            self.reqdetail_ = reqDetail

        return self.reqdetail_

    def get_failure_rate(self):
        result = self.get_reqdetail()

        frate = 0.
        wf_agents = result.get(self.name_, {}).get('AgentJobInfo', {})
        if not wf_agents:
            return frate

        nsuccess = 0
        nfailure = 0
        for agent, agentdata in wf_agents.items():
            status = agentdata.get('status', {})
            if not status: continue

            nsuccess += status.get('success', 0)

            for ftype, num in status.get('failure', {}).items():
                nfailure += num

        try:
            frate = float(nfailure) / (nfailure + nsuccess)
        except ZeroDivisionError:
            pass

        return frate

    def get_errors(self):
        output = {}

        jobdetail = self.get_jobdetail()
        if jobdetail.get('result', None):
            for step, stepdata in jobdetail['result'][0].get(self.name_,
                                                             {}).items():
                errors = {}
                for code, codedata in stepdata.get('jobfailed', {}).items():
                    sites = {}
                    for site, sitedata in codedata.items():
                        if sitedata['errorCount']:
                            sites[site] = sitedata['errorCount']

                    if sites: errors[code] = sites
                if errors: output[step] = errors

        acdc_server_response = get_json(
            self.url_,
            '/couchdb/acdcserver/_design/ACDC/_view/byCollectionName', {
                'key': f'"{self.name_}"',
                'include_docs': 'true',
                'reduce': 'false'
            },
            use_cert=True)

        for row in acdc_server_response.get('rows', []):
            task = row['doc']['fileset_name']

            new_output = output.get(task, {})
            new_errorcode = new_output.get('NotReported', {})
            for file_replica in row['doc']['files'].values():
                for site in file_replica['locations']:
                    new_errorcode[site] = 0

            new_output['NotReported'] = new_errorcode
            output[task] = new_output

        for step in list(output):
            if True in [(steptype in step)
                        for steptype in ['LogCollect', 'Cleanup']]:
                output.pop(step)

        return output


def test():
    import time

    wf0 = Workflow(
        'pdmvserv_task_HIG-RunIIAutumn18NanoAOD-00665__v1_T_190112_020711_2622'
    )
    wf1 = Workflow(
        'pdmvserv_task_HIG-RunIIAutumn18NanoAOD-00678__v1_T_190112_061134_937')

    print("<workflow 0>", wf0.name_)
    startTime = time.time()
    print("[job detail]", wf0.get_jobdetail())
    print("----- > took", time.time() - startTime, "s")
    startTime = time.time()
    print("[req detail]", wf0.get_reqdetail())
    print("----- > took", time.time() - startTime, "s")
    startTime = time.time()
    print("[failure rate]", wf0.get_failure_rate())
    print("----- > took", time.time() - startTime, "s")
    startTime = time.time()
    print("[errors]", wf0.get_errors())
    print("----- > took", time.time() - startTime, "s")

    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

    print("<workflow 1>", wf1.name_)
    startTime = time.time()
    print("[job detail]", wf1.get_jobdetail())
    print("----- > took", time.time() - startTime, "s")
    startTime = time.time()
    print("[req detail]", wf1.get_reqdetail())
    print("----- > took", time.time() - startTime, "s")
    startTime = time.time()
    print("[failure rate]", wf1.get_failure_rate())
    print("----- > took", time.time() - startTime, "s")
    startTime = time.time()
    print("[errors]", wf1.get_errors())
    print("----- > took", time.time() - startTime, "s")


if __name__ == "__main__":
    test()
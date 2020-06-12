#!/usr/bin/env python
"""simple wrapper around workflow, only has functions needed for monitoring
"""

from cmstoolbox import webtools
webtools.USER_AGENT = 'OSDroid'


class PrepID:
    def __init__(self, prepid, url="cmsweb.cern.ch"):
        self.name_ = prepid
        self.url_ = url

        result = webtools.get_json(self.url_, '/reqmgr2/data/request',
                          params={'prep_id': self.name_, 'detail': 'true'},
                          use_cert=True)
        result = result.get('result', [])
        self.data_ = result[0] if result else {}

    @property
    def name(self):
        return self.name_

    def __str__(self):
        return f"PrepID_{self.name}"

    def __repr__(self):
        return f"<PrepID {self.name}>"

    @property
    def workflows(self):
        """get list of workflow names associated with this prepid

        :return: list of names
        :rtype: list
        """
        return list(self.data_)


class Workflow:
    def __init__(self, wfname, url="cmsweb.cern.ch"):
        self.name_ = wfname
        self.url_ = url
        self.reqdetail_ = {}
        self.jobdetail_ = {}
        self.reqparams_ = {}

    @property
    def name(self):
        return self.name_

    def __str__(self):
        return f"Workflow_{self.name}"

    def __repr__(self):
        return f"<Workflow {self.name}>"

    def get_jobdetail(self):
        """fetch job detail from wmstatsserver, containing error info, available
        for running workflows.

        :return: job details
        :rtype: dict
        """
        if not self.jobdetail_:
            self.jobdetail_ = webtools.get_json(
                self.url_,
                f"/wmstatsserver/data/jobdetail/{self.name_}",
                use_cert=True)
        return self.jobdetail_

    def get_reqdetail(self):
        """fetch request details from wmstatsserver, available for running workflows.

        :return: request detail
        :rtype: dict
        """
        if not self.reqdetail_:
            reqDetail = {self.name_: dict()}
            raw = webtools.get_json(
                self.url_,
                f'/wmstatsserver/data/request/{self.name_}',
                use_cert=True)
            result = raw.get('result', None)
            if result is None:
                return reqDetail

            reqDetail[self.name_] = result[0].get(self.name_, {})
            self.reqdetail_ = reqDetail

        return self.reqdetail_

    def get_reqparams(self):
        """fetch workflow parameters from reqmgr2
        example: https://cmsweb.cern.ch/reqmgr2/data/request?name=pdmvserv_task_B2G-RunIIFall17wmLHEGS-00287__v1_T_180427_163824_4799

        :return: workflow parameters
        :rtype: dict
        """
        if not self.reqparams_:
            result = webtools.get_json(
                self.url_,
                '/reqmgr2/data/request',
                params={'name': self.name_},
                use_https=True,
                use_cert=True
            )
            for params in result['result']:
                for key, item in params.items():
                    if key == self.name_:
                        self.reqparams_ = item

        return self.reqparams_

    def get_prepid(self):
        """get prepid of workflow from req parameters,
        if fail to get, return empty string

        :return: prepid
        :rtype: str
        """
        return self.reqparams_.get('PrepID', '')

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

    @property
    def failureRate(self):
        return self.get_failure_rate()

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

        acdc_server_response = webtools.get_json(
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

    def get_total_estimated_jobs(self):
        return self.get_reqdetail().get(self.name_, {}).get("TotalEstimatedJobs", 0)

    @property
    def totalEstimatedJobs(self):
        return self.get_total_estimated_jobs()


def test():
    import time
    from pprint import pprint

    wf0 = Workflow('pdmvserv_task_HIG-RunIIAutumn18NanoAOD-00665__v1_T_190112_020711_2622')
    wf1 = Workflow('pdmvserv_task_HIG-RunIIAutumn18NanoAOD-00678__v1_T_190112_061134_937')
    prepid = PrepID('task_B2G-RunIIFall17wmLHEGS-00287')

    print("<workflow 0>", wf0)
    startTime = time.time()
    print("[job detail]")
    pprint(wf0.get_jobdetail())
    print("----- > took", time.time() - startTime, "s")
    startTime = time.time()
    print("[req detail]")
    pprint(wf0.get_reqdetail())
    print("----- > took", time.time() - startTime, "s")
    startTime = time.time()
    print("[failure rate]", wf0.get_failure_rate())
    print("----- > took", time.time() - startTime, "s")
    startTime = time.time()
    print("[errors]")
    pprint(wf0.get_errors())
    print("[total estimated jobs]", wf0.totalEstimatedJobs)
    print("----- > took", time.time() - startTime, "s")

    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

    print("<workflow 1>", wf1)
    startTime = time.time()
    print("[job detail]")
    pprint(wf1.get_jobdetail())
    print("----- > took", time.time() - startTime, "s")
    startTime = time.time()
    print("[req detail]")
    pprint(wf1.get_reqdetail())
    print("----- > took", time.time() - startTime, "s")
    startTime = time.time()
    print("[failure rate]", wf1.get_failure_rate())
    print("----- > took", time.time() - startTime, "s")
    startTime = time.time()
    print("[errors]")
    pprint(wf1.get_errors())
    print("[total estimated jobs]", wf1.totalEstimatedJobs)
    print("----- > took", time.time() - startTime, "s")

    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

    print("<PrepID>", prepid)
    print("[workflows]")
    pprint(prepid.workflows)


if __name__ == "__main__":
    test()

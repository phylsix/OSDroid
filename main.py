#!/usr/bin/env python

import json
import logging
import logging.config
import os
import time
import traceback
from os.path import abspath, dirname, join

import yaml
from monitutils import (get_workflow_from_db, get_yamlconfig, save_json,
                        update_doc_archive_db)
from workflowalerts import alertWithEmail, errorEmailShooter
from workflowlabelmaker import updateLabelArchives
from workflowmonitexporter import (buildDoc, prepareWorkflows, sendDoc,
                                   updateWorkflowStatusToDb)
from workflowprediction import makingPredictionsWithML

LOGDIR = join(dirname(abspath(__file__)), 'Logs')
CRED_FILE_PATH = join(dirname(abspath(__file__)), 'config/credential.yml')
CONFIG_FILE_PATH = join(dirname(abspath(__file__)), 'config/config.yml')
LOGGING_CONFIG = join(dirname(abspath(__file__)), 'config/configLogging.yml')

logger = logging.getLogger("workflowmonitLogger")
rootlogger = logging.getLogger()


class No502WarningFilter(logging.Filter):
    def filter(self, record):
        return 'STATUS: 502' not in record.getMessage()
rootlogger.addFilter(No502WarningFilter())


def main():

    logging.config.dictConfig(get_yamlconfig(LOGGING_CONFIG))
    cred = get_yamlconfig(CRED_FILE_PATH)
    localconfig = get_yamlconfig(CONFIG_FILE_PATH)

    if not os.path.isdir(LOGDIR):
        os.makedirs(LOGDIR)

    recipients = localconfig.get('alert_recipients', [])

    try:
        wfpacks = prepareWorkflows(CONFIG_FILE_PATH, test=False)
        totaldocs = []
        for pack in wfpacks:
            try:
                docs = buildDoc(pack, doconcurrent=True)
                totaldocs.extend(docs)

                # update status in local db
                updateWorkflowStatusToDb(CONFIG_FILE_PATH, docs)
                # send to CERN MONIT
                failures = sendDoc(cred, docs)
                # alerts
                alertWithEmail(docs, recipients)

                # backup doc
                # bkpfn = join(LOGDIR, 'toSendDoc_{}'.format(time.strftime('%y%m%d-%H%M%S')))
                # bkpdoc = save_json(docs, filename=bkpfn, gzipped=True)
                # logger.info('Document backuped at: {}'.format(bkpdoc))

                # backup failure msg
                if len(failures):
                    faildocfn = join(LOGDIR, 'amqFailMsg_{}'.format(time.strftime('%y%m%d-%H%M%S')))
                    faildoc = save_json(failures, filename=faildocfn, gzipped=True)
                    logger.info('Failed message saved at: {}'.format(faildoc))

                logger.info('Number of updated workflows: {}'.format(len(docs)))
            except Exception:
                logger.exception(f"Exception encountered, sending emails to {str(recipients)}")
                errorEmailShooter(traceback.format_exc(), recipients)

        # predictions
        logger.info("Making predicions for {} workflows..".format(len(totaldocs)))
        makingPredictionsWithML(totaldocs)

        # labeling
        qcmd = "SELECT NAME FROM CMS_UNIFIED_ADMIN.WORKFLOW WHERE WM_STATUS LIKE '%archived'"
        archivedwfs = get_workflow_from_db(CONFIG_FILE_PATH, qcmd)
        _wfnames = [w.name for w in archivedwfs]
        logger.info("Passing {} workflows for label making..".format(len(_wfnames)))
        updateLabelArchives(_wfnames)

        # archive docs:
        docs_to_insert = [(doc['name'], json.dumps(doc)) for doc in totaldocs]
        update_doc_archive_db(localconfig, docs_to_insert)

    except Exception:
        logger.exception(f"Exception encountered, sending emails to {str(recipients)}")
        errorEmailShooter(traceback.format_exc(), recipients)


def test():
    logging.config.dictConfig(get_yamlconfig(LOGGING_CONFIG))
    logger = logging.getLogger("testworkflowmonitLogger")

    if not os.path.isdir(LOGDIR):
        os.makedirs(LOGDIR)

    cred = get_yamlconfig(CRED_FILE_PATH)
    recipients = get_yamlconfig(CONFIG_FILE_PATH).get('alert_recipients', [])

    try:

        wfpacks = prepareWorkflows(CONFIG_FILE_PATH, test=True)
        totaldocs = []
        for pack in wfpacks:
            docs = buildDoc(pack, doconcurrent=True)
            totaldocs.extend(docs)

        # predictions
        logger.info("Making predicions for {} workflows..".format(len(totaldocs)))
        makingPredictionsWithML(totaldocs)
        # labeling
        qcmd = "SELECT NAME FROM CMS_UNIFIED_ADMIN.WORKFLOW WHERE WM_STATUS LIKE '%archived'"
        archivedwfs = get_workflow_from_db(CONFIG_FILE_PATH, qcmd)
        _wfnames = [w.name for w in archivedwfs]
        logger.info("Passing {} workflows for label making..".format(len(_wfnames)))
        updateLabelArchives(_wfnames)

    except Exception:
        logger.exception(f"Exception encountered, sending emails to {str(recipients)}")


if __name__ == "__main__":
    main()

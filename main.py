#!/usr/bin/env python

import os
from os.path import join, dirname, abspath
import logging
import logging.config
import time

import yaml
from monitutils import get_yamlconfig, save_json
from workflowmonitexporter import buildDoc, prepareWorkflows, updateWorkflowStatusToDb, sendDoc
from workflowalerts import alertWithEmail, errorEmailShooter
from workflowprediction import makingPredictionsWithML
from workflowlabelmaker import updateLabelArchives

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

    with open(LOGGING_CONFIG, 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)

    if not os.path.isdir(LOGDIR):
        os.makedirs(LOGDIR)

    cred = get_yamlconfig(CRED_FILE_PATH)
    recipients = get_yamlconfig(CONFIG_FILE_PATH).get('alert_recipients', [])

    try:

        wfpacks = prepareWorkflows(CONFIG_FILE_PATH, test=False)
        totaldocs = []
        for pack in wfpacks:
            docs = buildDoc(pack, doconcurrent=True)
            totaldocs.extend(docs)

            # update status in local db
            updateWorkflowStatusToDb(CONFIG_FILE_PATH, docs)
            # send to CERN MONIT
            failures = sendDoc(cred, docs)
            # alerts
            alertWithEmail(docs, recipients)

            # backup doc
            bkpfn = join(LOGDIR, 'toSendDoc_{}'.format(time.strftime('%y%m%d-%H%M%S')))
            bkpdoc = save_json(docs, filename=bkpfn, gzipped=True)
            logger.info('Document backuped at: {}'.format(bkpdoc))

            # backup failure msg
            faildocfn = join(
                LOGDIR, 'amqFailMsg_{}'.format(time.strftime('%y%m%d-%H%M%S')))
            if len(failures):
                faildoc = save_json(failures, filename=faildocfn, gzipped=True)
                logger.info('Failed message saved at: {}'.format(faildoc))

            logger.info('Number of updated workflows: {}'.format(len(docs)))

        # predictions
        makingPredictionsWithML(totaldocs)
        # labeling
        updateLabelArchives([wf for pack in wfpacks for wf in pack])

    except Exception as e:
        logger.exception(f"Exception encountered, sending emails to {str(recipients)}")
        errorEmailShooter(str(e), recipients)


if __name__ == "__main__":
    main()

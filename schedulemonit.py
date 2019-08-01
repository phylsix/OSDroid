#!/usr/bin/env python
"""schedule the monitoring task in the background
"""

from os.path import join, dirname, abspath
import time

import schedule
from main import main as run


LOGDIR = join(dirname(abspath(__file__)), 'Logs')
CRED_FILE_PATH = join(dirname(abspath(__file__)), 'config/credential.yml')
CONFIG_FILE_PATH = join(dirname(abspath(__file__)), 'config/config.yml')
LOGGING_CONFIG = join(dirname(abspath(__file__)), 'config/configLogging.yml')

schedule.every(50).minutes.do(run)


if __name__ == "__main__":

    while True:
        schedule.run_pending()
        time.sleep(1)

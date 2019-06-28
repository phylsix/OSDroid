#!/bin/bash

export X509_USER_PROXY=/home/wsi/private/monitcert
python schedulemonit.py&
disown %1
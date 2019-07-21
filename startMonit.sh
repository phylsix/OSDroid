#!/bin/bash

export X509_USER_PROXY=/home/wsi/private/monitcert
nohup python schedulemonit.py&

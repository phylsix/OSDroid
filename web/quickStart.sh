#!/bin/bash

export FLASK_APP=__init__.py
nohup flask run --host=0.0.0.0 --port=8020 > OSDroidWebInterface.log 2>&1 &

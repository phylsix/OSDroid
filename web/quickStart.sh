#!/bin/bash

export FLASK_APP=webapp.py
flask run --host=0.0.0.0 --port=8020 &
disown %1

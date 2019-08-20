#!/bin/bash
HOME=.
VENV_DIR=.virtualenvs

# Deploy cicada-scheduler in Python3 virtual environment
python3 -m venv $VENV_DIR
source $VENV_DIR/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install croniter psycopg2-binary pyyaml tabulate
deactivate

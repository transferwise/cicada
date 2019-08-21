#!/bin/bash

# Deploy cicada-scheduler in Python3 virtual environment
python3 -m venv .virtualenvs
source .virtualenvs/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install croniter psycopg2-binary pyyaml tabulate
deactivate

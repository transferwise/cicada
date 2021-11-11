#!/bin/bash

# Use virtual environment for Python3
python3 -m venv .virtualenvs
source .virtualenvs/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install croniter psycopg2-binary pyyaml tabulate
deactivate

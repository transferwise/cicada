#!/bin/bash
HOME=/opt/app
VENV_DIR=$HOME/cicada-scheduler/.virtualenvs

# Deploy cicada-scheduler in Python3 virtual environment
python3 -m venv $VENV_DIR
source $VENV_DIR/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install croniter psycopg2-binary pyyaml tabulate
deactivate

echo
echo "--------------------------------------------------------------------------"
echo "Cicada Scheduler Intiated - Next Steps"
echo "--------------------------------------------------------------------------"
echo
echo "cp $HOME/config/example.yml $HOME/config/definitions.yml"
echo "vim $HOME/config/definitions.yml"
echo "$VENV_DIR/bin/python3 $HOME/bin/registerServer.py"
echo "echo \"* * * * * $VENV_DIR/bin/python3 $HOME/bin/findSchedules.py\" | crontab "
echo

DIR=$(pwd)
VENV_DIR=$DIR/.virtualenvs

# Deploy cicada-scheduler in Python3 virtual environment
python3 -m venv $VENV_DIR
source $VENV_DIR/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install croniter psycopg2-binary pyyaml
deactivate

echo
echo "--------------------------------------------------------------------------"
echo "Cicada Scheduler Intiated - Next Steps"
echo "--------------------------------------------------------------------------"
echo
echo "cp $DIR/config/example.yml $DIR/config/definitions.yml"
echo "vim $DIR/config/definitions.yml"
echo "$VENV_DIR/bin/python3 $DIR/bin/registerServer.py"
echo "echo \"* * * * * $VENV_DIR/bin/python3 $DIR/bin/findSchedules.py\" | crontab "
echo
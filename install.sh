DIR=$(pwd)

# Deploy cicada-scheduler in Python3 virtual environment
python3 -m venv .virtualenvs
source $DIR/.virtualenvs/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install croniter psycopg2-binary pyyaml
deactivate

echo
echo "--------------------------------------------------------------------------"
echo "Cicada Scheduler Intiated - Next Steps"
echo "--------------------------------------------------------------------------"
echo
echo "1. cp $DIR/config/example.yml $DIR/config/definitions.yml"
echo "2. vim $DIR/config/definitions.yml"
echo "3. echo \"* * * * * $DIR/.virtualenvs/bin/python3 $DIR/bin/findSchedules.py\" | crontab "
echo "4. $DIR/.virtualenvs/bin/python3 $DIR/bin/registerServer.py"
echo
DIR=$(pwd)

# Deploy cicada-scheduler in Python3 virtual environment
python3 -m .virtualenvs $DIR
source $DIR/.virtualenvs/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install croniter psycopg2-binary pyyaml
deactivate

echo
echo "--------------------------------------------------------------------------"
echo "Cicada Scheduler Intiated - Next Steps"
echo "--------------------------------------------------------------------------"
echo
echo "1. cp $DIR/cicada-scheduler/config/example.yml $DIR/cicada-scheduler/config/definitions.yml"
echo "2. vim $DIR/cicada-scheduler/config/definitions.yml"
echo "3. echo \"* * * * * $DIR/.virtualenvs/bin/python3 $DIR/bin/findSchedules.py\" | crontab "
echo "4. $DIR/bin/python3 $DIR/cicada-scheduler/bin/registerServer.py"
echo
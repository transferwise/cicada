#!/usr/bin/env bash

set -e

# Set some bashrc
cat >~/.bashrc <<EOL
# enable color support of ls and also add handy aliases
if [ -x /usr/bin/dircolors ]; then
    test -r ~/.dircolors && eval "$(dircolors -b ~/.dircolors)" || eval "$(dircolors -b)"
    alias ls='ls --color=auto'
    alias grep='grep --color=auto'
    alias fgrep='fgrep --color=auto'
    alias egrep='egrep --color=auto'
fi
# some more ls aliases
alias ll='ls -alF'
alias la='ls -A'
alias l='ls -CF'

source /opt/cicada/venv/bin/activate

EOL


# Install OS dependencies
apt-get update
apt-get install -y --no-install-recommends \
  postgresql-client \
  vim


# Change to Cicada folder
cd ${CICADA_HOME}
pwd


# Build backend database
export PGPASSWORD=${DB_POSTGRES_PASS}
psql -v ON_ERROR_STOP=1 -U${DB_POSTGRES_USER} -h${DB_POSTGRES_HOST} -p${DB_POSTGRES_PORT} ${DB_POSTGRES_DB} --file=setup/schema.sql --quiet

# If not exists, create definitions file for cli
test -f ${CICADA_HOME}/config/definitions.yml || cat > ${CICADA_HOME}/config/definitions.yml <<EOL
db_cicada:
    host: ${DB_POSTGRES_HOST}
    port: ${DB_POSTGRES_PORT}
    dbname: ${DB_POSTGRES_DB}
    user: ${DB_POSTGRES_USER}
    password: ${DB_POSTGRES_PASS}

slack:
    token: xyz48945
    channel: channel_name
EOL

# Install Cicada dev(and test) environment
make dev --file=${CICADA_HOME}/Makefile --always-make python=python3.8


# Register this server in Database
${CICADA_HOME}/venv/bin/cicada register_server

# Upsert some manual test schedules
${CICADA_HOME}/venv/bin/cicada upsert_schedule --schedule_id=missing_exec --is_enabled=1 --exec_command="death.exe" --interval_mask="* * * * *"
${CICADA_HOME}/venv/bin/cicada upsert_schedule --schedule_id=missing_operand --is_enabled=1 --exec_command="sleep" --interval_mask="* * * * *"
${CICADA_HOME}/venv/bin/cicada upsert_schedule --schedule_id=sleep --is_enabled=1 --exec_command="sleep" --parameters="0.5" --interval_mask="* * * * *"
${CICADA_HOME}/venv/bin/cicada upsert_schedule --schedule_id=sleep600 --is_enabled=1 --exec_command="sleep" --parameters="600" --interval_mask="* * * * *"
${CICADA_HOME}/venv/bin/cicada upsert_schedule --schedule_id=poll_sleep600 --is_enabled=1 --exec_command="python3 ${CICADA_HOME}/tests/utils/poll_sleep600.py" --interval_mask="* * * * *"


# # Use linux CRON to find new schedules every minute
# echo
# echo "Use linux CRON to find new schedules every minute"
# apt-get install -y --no-install-recommends cron
# service cron start
# echo "* * * * * ${CICADA_HOME}/venv/bin/python3 ${CICADA_HOME}/venv/bin/cicada exec_server_schedules" | crontab


echo
echo "=========================================================================="
echo "Cicada Dev environment is ready in Docker container(s)."
echo
echo "Running containers"
echo "------------------"
echo " * cicada_dev"
echo " * db_cicada"
echo
echo "Postgres backend"
echo "----------------"
echo " from local-dev host       - localhost:${DB_POSTGRES_PORT_ON_HOST}"
echo " from cicada_dev contanier - ${DB_POSTGRES_HOST}:${DB_POSTGRES_PORT}"
echo " (check .env file for credentials)"
echo
echo
echo "=========================================================================="
echo "Log into cicada_dev container"
echo "-----------------------------"
echo " $ docker exec -it cicada_dev bash"
echo
echo "Run cicada in cicada_dev container"
echo "----------------------------------"
echo "(Re)build Cicada env  : make dev --file=${CICADA_HOME}/Makefile --always-make"
echo "Activate virtual env  : source ${CICADA_HOME}/venv/bin/activate"
echo "Help                  : cicada -h"
echo "List server schedules : cicada list_server_schedules"
echo
echo "Run tests in cicada_dev container"
echo "---------------------------------"
echo "Run tests             : docker exec -it cicada_dev make pytest"
echo "Run linters           : docker exec -it cicada_dev make dev flake8 black"
echo "=========================================================================="


# Stay in container and output logs
tail -f /dev/null

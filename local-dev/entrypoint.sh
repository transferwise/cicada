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
EOL

# Install OS dependencies
apt-get update
apt-get install -y --no-install-recommends \
  postgresql-client \
  vim

# rm -rf /var/lib/apt/lists/* 

# Change to Cicada folder
echo ${CICADA_HOME}
cd ${CICADA_HOME}
pwd

# Build backend database
export PGPASSWORD=${DB_POSTGRES_PASSWORD}
psql -U${DB_POSTGRES_USER} -h${DB_POSTGRES_HOST} -p${DB_POSTGRES_PORT} ${DB_POSTGRES_DB} --file=setup/schema.sql --quiet

# Create definitions file for Cicada to connect to Postgres backend
cat >config/definitions.yml <<EOL
db_cicada:
    host: ${DB_POSTGRES_HOST}
    port: ${DB_POSTGRES_PORT}
    dbname: ${DB_POSTGRES_DB}
    user: ${DB_POSTGRES_USER}
    password: ${DB_POSTGRES_PASSWORD}
EOL

# Install Cicada Scheduler
bash install.sh

# Register new Node in Database
${CICADA_HOME}/.virtualenv/bin/python3 ${CICADA_HOME}/bin/registerServer.py

# Create a schedule
${CICADA_HOME}/.virtualenv/bin/python3 ${CICADA_HOME}/bin/manageSchedule.py upsert --scheduleId=wait --isEnabled=1 --execCommand="${CICADA_HOME}/.virtualenv/bin/python3 ${CICADA_HOME}/bin/waitSomeSeconds.py" --parameters="3" --intervalMask='* * * * *'

# Add linux CRON job to check central scheduler every minute
# echo "* * * * * ${CICADA_HOME}/.virtualenv/bin/python3 ${CICADA_HOME}/bin/findSchedules.py" | crontab

echo
echo "=========================================================================="
echo "Cicada Dev environment is ready in Docker container(s)."
echo
echo "Running containers"
echo "------------------"
echo " * cicada_dev"
echo " * cicada_db"
echo
echo "Postgres backend"
echo "----------------"
echo " from local-dev host       - localhost:${DB_POSTGRES_PORT_ON_HOST}"
echo " from cicada_dev contanier - ${DB_POSTGRES_HOST}:${DB_POSTGRES_PORT}"
echo " (check .env file for credentials)"
echo
echo "Log into cicada_dev node"
echo "------------------------"
echo " $ docker exec -it cicada_dev bash"
echo
echo "From within cicada_dev node"
echo "---------------------------"
echo "Show scheduled jobs : $ ${CICADA_HOME}/.virtualenv/bin/python3 ${CICADA_HOME}/bin/showSchedules.py"
echo "Run scheduled jobs  : $ ${CICADA_HOME}/.virtualenv/bin/python3 ${CICADA_HOME}/bin/findSchedules.py"
echo
echo "=========================================================================="

# Continue running the container
tail -f /dev/null

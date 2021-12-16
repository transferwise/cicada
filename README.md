# Cicada scheduler
*Centralised Distributed Scheduler*

Requires a central database for setting up and monitoring simple schedules for nodes

Nodes are responsible for retrieving and executing schedules

Moving schedules from one node to another is centralised and trivial

Terminating running schedules is trivial

## Setup

### Central Database

Verified on **PostgreSQL** versions *9.6* to *12.9*

1. Execute as **postgres** user [setup/db_and_user.sql](setup/db_and_user.sql)
2. Change **cicada** user password
3. Execute as **cicada** user [setup/schema.sql](setup/schema.sql)



### Scheduler Node

#### Ubuntu Node

Verified on *Ubuntu 18.04 and 20.04 LTS*

Prerequisites

- ntpd
- python3.8

```bash
# Install Cicada Scheduler
sudo mkdir -p /opt/cicada
sudo chown -R $USER:$USER /opt/cicada
cd /opt/cicada
DIR=$(pwd)

cd $DIR
git clone git@github.com:transferwise/cicada.git .
make

# Update the db_cicada section of the environmental config file
cp $DIR/config/example.yml $DIR/config/definitions.yml
vim $DIR/config/definitions.yml

# Register new Node in Database
$DIR/venv/bin/cicada register_server

# Add linux CRON job to check central scheduler every minute
echo "* * * * * $DIR/venv/bin/python3 $DIR/venv/bin/cicada exec_server_schedules" | crontab
```

## Administration

![erd](docs/erd.png)

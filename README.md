# cicada-scheduler
*Centralised Distributed Scheduler*

Requires a central database for setting up and monitoring simple schedules for nodes

Nodes are responsible for retrieving and executing schedules

Moving schedules from one node to another is centralized and trivial

## Setup

### Central Database

Verified on **PostgreSQL** versions *9.6* to *11.1*

1. Execute as **postgres** user [setup/db_and_user.sql](setup/db_and_user.sql)
2. Change **cicada** user password
3. Execute as **cicada** user [setup/schema.sql](setup/schema.sql)



### Scheduler Node

#### Ubuntu Node

Verified on *Ubuntu 18.04.1 LTS*

Prerequisites

- ntpd

```bash
# Add required Python3 components
apt install -y git python3 python3-pip python3-venv
apt upgrade -y

# initiate cicada-scheduler
mkdir /opt/app
cd /opt/app
git clone git@github.com:transferwise/cicada-scheduler.git
cd cicada-scheduler
./install.sh

# Update the db_cicada section of the environmental config file
cp /opt/cicada-venv/cicada-scheduler/config/example.yml /opt/cicada-venv/cicada-scheduler/config/definitions.yml
vim /opt/cicada-venv/cicada-scheduler/config/definitions.yml

# Add linux CRON job to check central scheduler every minute
adduser cicada --disabled-login --gecos ""
sudo su - cicada
echo "* * * * * /opt/app/cicada-scheduler/.virtualenvs/bin/python3 /opt/app/cicada-scheduler/bin/findSchedules.py" | crontab
exit
```

#### Register new Node in Database

##### Method1

```bash
/opt/cicada-venv/bin/python3 /opt/cicada-venv/cicada-scheduler/bin/registerServer.py
```

##### Method2

```sql
INSERT INTO servers
  (hostname, fqdn, ip4_address)
VALUES
  ('{hostname}', '{fqdn}', '{ip4_address}')
;
```

## Administration

![erd](/docs/erd.png)

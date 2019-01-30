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

# Deploy cicada-scheduler in Python3 virtual environment
python3 -m venv /opt/cicada-venv
source /opt/cicada-venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install croniter psycopg2-binary pyyaml
git clone git@github.com:transferwise/cicada-scheduler.git /opt/cicada-venv/cicada-scheduler

# Update the db_cicada section of the environmental config file
cp /opt/cicada-venv/cicada-scheduler/config/example.yml /opt/cicada-venv/cicada-scheduler/config/definitions.yml
vim /opt/cicada-venv/cicada-scheduler/config/definitions.yml

# Add linux CRON job to check central scheduler every minute
adduser cicada --disabled-login --gecos ""
sudo su - cicada
echo "* * * * * /opt/cicada-venv/bin/python3 /opt/cicada-venv/cicada-scheduler/bin/findSchedules.py" | crontab
exit
```



#### CentOS Node

Verified on *CentOS Linux release 7.6.1810 (Core)*

Prerequisites

* epel-release
* ntpd

```bash
# Add required Python3 components
yum install -y git python36
yum upgrade -y

# Deploy cicada-scheduler in Python3 virtual environment
python36 -m venv /opt/cicada-venv
source /opt/cicada-venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install croniter psycopg2-binary pyyaml
git clone git@github.com:transferwise/cicada-scheduler.git /opt/cicada-venv/cicada-scheduler

# Update the db_cicada section of the environmental config file
cp /opt/cicada-venv/cicada-scheduler/config/example.yml /opt/cicada-venv/cicada-scheduler/config/definitions.yml
vim /opt/cicada-venv/cicada-scheduler/config/definitions.yml

# Add linux CRON job to check central scheduler every minute
adduser cicada
sudo su - cicada
echo "* * * * * /opt/cicada-venv/bin/python3 /opt/cicada-venv/cicada-scheduler/bin/findSchedules.py" | crontab
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

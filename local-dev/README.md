# Sample Project for Docker Development Environment

The local development environment comes with the following containers:
* cicada_db : Postgres database to use as backend
* cicada_dev : Python 3.6.9 with Cicada compatible python virtual environment

## How to use

Install [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/).

Go to the main folder of the repository (the parent of this one) and To create local development environment:

```sh
$ cd local-dev/
$ docker-compose up --build
```

Wait until `Cicada Dev environment is ready in Docker container(s).` message. At the first run this can
run up to 5-10 minutes depending on your computer and your network connection. Once it's completed every
container, virtual environment and environment variables are set configured.

Open another terminal and shell into the Cicada container:

```sh
$ docker exec -it cicada_dev bash
```
Show node scheduled jobs
``` sh
$ ${CICADA_HOME}/.virtualenv/bin/python3 ${CICADA_HOME}/bin/showSchedules.py
```

Run node scheduled jobs
```sh
$ ${CICADA_HOME}/.virtualenv/bin/python3 ${CICADA_HOME}/bin/findSchedules.py
```

**Note**: 
If you want to connect to the backend databases by a db client (CLI, MySQL Workbench, pgAdmin, intelliJ, DataGrip, etc.),
check the [local-dev/.env](../local-dev/.env) file for the credentials.

###  Running tests

Not currently available

###  Configuring end to end tests

Not currently available

### To refresh the containers

To refresh the containers with new local code changes stop the running instances with `ctrl+c` and restart as usual:

```sh
$ docker-compose up --build
```

# Sample Project for Docker Development Environment

The local development environment comes with the following containers:
* db_cicada : Postgres database to use as backend
* cicada_dev : Python 3.8 with Cicada compatible python virtual environment

## How to use

Install [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/).

Go to the main folder of the repository (the parent of this one) and To create local development environment:

```sh
$ cd local-dev/
$ docker-compose up --build
```

Wait until `Cicada Dev environment is ready in Docker container(s).` message. At the first run this can
run up to 5-10 minutes depending on your computer and your network connection. Once it's completed every container, virtual environment and environment variables are set configured.

Open another terminal and shell into the Cicada container:

```sh
$ docker exec -it cicada_dev bash
```

Show node scheduled jobs
``` sh
$ ${CICADA_HOME}/venv/bin/cicada show_schedules
```

**Note**:
If you want to connect to the backend databases by a db client (CLI, pgAdmin, etc.),
check the [local-dev/.env](../local-dev/.env) file for the credentials.

###  Running tests

``` sh
$ docker exec -it cicada_dev make pytest
```

###  Running linters

``` sh
$ make dev pylint flake8 black
```

### To refresh the containers

To refresh the containers with new local code changes stop the running instances with `ctrl+c` and restart as usual:

```sh
$ ./refresh-local-dev.sh
$ docker-compose up
```

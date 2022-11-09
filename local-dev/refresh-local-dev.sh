#!/usr/bin/bash

docker compose down
docker system prune --volumes --force
sudo rm ../venv/ -Rf


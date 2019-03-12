#!/bin/sh

set -e
set -u
set -x

DIR="$(dirname $0)"

dc() {
	docker-compose -p parkeervakken${ENVIRONMENT} -f ${DIR}/docker-compose.yml $*
}

trap 'dc kill ; dc down ; dc rm -f' EXIT

echo "Removing any previous backups"
rm -rf ${DIR}/backups
mkdir -p ${DIR}/backups

echo "Building dockers"
dc down
dc rm -f
dc pull
dc build

echo "Starting and migrating db"
dc up -d database
dc run importer /deploy/docker-wait.sh

echo "Importing data"
dc run --rm importer

echo "Running backups"
dc exec -T database backup-db.sh parkeervakken
dc down -v
echo "Done"

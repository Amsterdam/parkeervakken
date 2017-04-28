#!/usr/bin/env bash

set -u
set -e

# wait for postgres
while ! nc -z ${PARKEERVAKKEN_DB_PORT_5432_TCP_ADDR} ${PARKEERVAKKEN_DB_PORT_5432_TCP_PORT}
do
	echo "Waiting for postgres..."
	sleep 0.5
done

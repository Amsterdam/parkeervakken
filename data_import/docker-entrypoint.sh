#!/bin/bash

# wait for database to load
source docker-wait.sh

unzip $(ls -Art data/*.zip | tail -n 1) -d /app/unzipped/


# clear and or create tables
python import_data.py --user $PARKEERVAKKEN_DB_USER \
		      --password $PARKEERVAKKEN_DB_PASSWORD \
		      --host $PARKEERVAKKEN_DB_PORT_5432_TCP_ADDR \
		      --port $PARKEERVAKKEN_DB_PORT_5432_TCP_PORT \
		      --database parkeervakken \
                      initialize

# run import / update data
python import_data.py --user $PARKEERVAKKEN_DB_USER \
		      --password $PARKEERVAKKEN_DB_PASSWORD \
		      --host $PARKEERVAKKEN_DB_PORT_5432_TCP_ADDR \
		      --port $PARKEERVAKKEN_DB_PORT_5432_TCP_PORT \
		      --database parkeervakken \
                      update \
                      --source /app/unzipped

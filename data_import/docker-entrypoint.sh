#!/bin/bash

set -e
set -u

echo $PARKEERVAKKEN_DB_PORT_5432_TCP_ADDR
echo $PARKEERVAKKEN_DB_PORT_5432_TCP_PORT

# wait for database to load
source docker-wait.sh

# Get files stored in the objectstore
echo "Getting zip files from objectstore"
python ./objectstore.py

echo 'unzipping latest source shape file'
unzip -o $(ls -Art data/parkeren/* | grep [0-9].zip | tail -n 1) -d /app/unzipped/

count=$(ls /app/unzipped/*shp -l | wc -l)

echo $count shapefiles

if [ "$count" -lt '8' ]; then
    echo "missing shp files";
    exit
fi

unzip -o $(ls -Art data/parkeren/*niet*fiscaal*.zip | tail -n 1) -d /app/unzipped/nietfiscaal

echo 'clear / build tables'
# clear and or create tables
python import_data.py --user $PARKEERVAKKEN_DB_USER \
		      --password $PARKEERVAKKEN_DB_PASSWORD \
		      --host $PARKEERVAKKEN_DB_PORT_5432_TCP_ADDR \
		      --port $PARKEERVAKKEN_DB_PORT_5432_TCP_PORT \
		      --database parkeervakken \
                      initialize

echo 'Load parkeer data'
# run import / update data
python import_data.py --user $PARKEERVAKKEN_DB_USER \
		      --password $PARKEERVAKKEN_DB_PASSWORD \
		      --host $PARKEERVAKKEN_DB_PORT_5432_TCP_ADDR \
		      --port $PARKEERVAKKEN_DB_PORT_5432_TCP_PORT \
		      --database parkeervakken \
                      update \
                      --source /app/unzipped

echo 'load parkeer NIET FISCAAL data'
# run import / update data
python import_data.py --user $PARKEERVAKKEN_DB_USER \
		      --password $PARKEERVAKKEN_DB_PASSWORD \
		      --host $PARKEERVAKKEN_DB_PORT_5432_TCP_ADDR \
		      --port $PARKEERVAKKEN_DB_PORT_5432_TCP_PORT \
		      --database parkeervakken \
                      update \
                      --source /app/unzipped/nietfiscaal \
                      --skip-dates \


echo 'parkeerdata DONE'

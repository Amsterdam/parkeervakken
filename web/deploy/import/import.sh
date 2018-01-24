#!/bin/bash

set -u
set -e

DATABASE_HOST="database"
DATABASE_PORT=5432

# LOCAL TESTING:
# DATABASE_HOST="127.0.0.1"
# DATABASE_PORT=5409

echo $DATABASE_HOST
echo $DATABASE_PORT

# Get files stored in the objectstore
echo "Getting zip files from objectstore"
python /deploy/import/objectstore.py

echo 'unzipping latest source shape file'
unzip -o $(ls -Art /data/parkeren/* | grep [0-9].zip | tail -n 1) -d /unzipped/

count=$(ls /unzipped/*shp -l | wc -l)

echo $count shapefiles

if [ "$count" -lt '8' ]; then
    echo "missing shp files";
    exit
fi

unzip -o $(ls -Art /data/parkeren/*niet*fiscaal*.zip | tail -n 1) -d /unzipped/nietfiscaal

echo 'clear / build tables'
# clear and or create tables
python /deploy/import/import_data.py --user $DATABASE_USER \
		      --password $DATABASE_PASSWORD \
		      --host $DATABASE_HOST \
		      --port $DATABASE_PORT \
		      --database parkeervakken \
                      initialize

echo 'Load parkeer data'
# run import / update data
python /deploy/import/import_data.py --user $DATABASE_USER \
		      --password $DATABASE_PASSWORD \
		      --host $DATABASE_HOST \
		      --port $DATABASE_PORT \
		      --database parkeervakken \
                      update \
                      --source /unzipped

echo 'load parkeer NIET FISCAAL data'
# run import / update data
python /deploy/import/import_data.py --user $DATABASE_USER \
		      --password $DATABASE_PASSWORD \
		      --host $DATABASE_HOST \
		      --port $DATABASE_PORT \
		      --database parkeervakken \
                      update \
                      --source /unzipped/nietfiscaal \
                      --skip-dates \

echo 'parkeerdata DONE'
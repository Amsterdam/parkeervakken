#!/bin/bash

set -u
set -e

/usr/local/bin/python /import_data.py --user $DB_NAME \
                                      --password $DB_PASSWORD \
                                      --host $DATABASE_PORT_5432_TCP_ADDR \
                                      --port $DATABASE_PORT_5432_TCP_PORT \
                                      --database parkeervakken \
                                      update \
                                      --source /app/diva

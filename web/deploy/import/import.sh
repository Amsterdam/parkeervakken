#!/bin/bash

set -u
set -e

python /deploy/import/import_data.py --user $DATABASE_USER \
		       --password $DATABASE_PASSWORD \
		       --host database \
		       --port 5432 \
		       --database parkeervakken \
		       update \
		       --source /app/diva

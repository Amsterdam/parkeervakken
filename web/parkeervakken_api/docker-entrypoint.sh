#!/usr/bin/env bash

set -u   # crash on missing env variables
set -e   # stop on any error

cd /app

source docker-wait.sh

# collect static files
python manage.py collectstatic --noinput

# reset geo_views
python manage.py migrate geo_views zero

# migrate database tables
yes yes | python manage.py migrate --noinput

# run uwsgi
exec uwsgi --ini /app/uwsgi.ini

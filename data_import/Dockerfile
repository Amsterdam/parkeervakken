FROM python:3.5
MAINTAINER datapunt@amsterdam.nl

ENV PYTHONUNBUFFERED 1

WORKDIR /app

COPY import_data.py        /app/import_data.py
COPY create_his_tables.sql /app/create_his_tables.sql
COPY create_bm_tables.sql  /app/create_bm_tables.sql
COPY create_bv_tables.sql  /app/create_bv_tables.sql
COPY update_his.sql        /app/update_his.sql
COPY import_his_bm.sql     /app/import_his_bm.sql
COPY import_bm_bv.sql      /app/import_bm_bv.sql
COPY import.sh             /app/import.sh
COPY requirements.txt      /app/requirements.txt

#RUN chmod +x /import.sh

RUN apt-get update && \
	apt-get install -y libgeos-dev && \
        apt-get install -y postgis && \
	adduser --system datapunt && \
	mkdir -p /static && \
	chown datapunt /static && \
	pip install uwsgi

#RUN apt-get update \
#    && apt-get install -y postgis


RUN pip install --no-cache-dir -r requirements.txt

COPY docker-entrypoint.sh /app/docker-entrypoint.sh

CMD /app/docker-entrypoint.sh

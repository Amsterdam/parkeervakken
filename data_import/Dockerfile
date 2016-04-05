FROM python:3.5
MAINTAINER datapunt@amsterdam.nl

ENV PYTHONUNBUFFERED 1


#RUN chmod +x /import.sh

RUN apt-get update && apt-get install -y \
	libgeos-dev \
        postgis \
	netcat \
	unzip

RUN adduser --system datapunt && \
	mkdir -p /static && \
	mkdir -p /unzipped && \
	chown datapunt /static && \
	pip install uwsgi


WORKDIR /app

COPY requirements.txt      /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py                  /app/
COPY *.sh                  /app/
COPY *.sql                 /app/

#COPY import_data.py        /app/import_data.py
#COPY create_his_tables.sql /app/create_his_tables.sql
#COPY create_bm_tables.sql  /app/create_bm_tables.sql
#COPY create_bv_tables.sql  /app/create_bv_tables.sql
#COPY update_his.sql        /app/update_his.sql
#COPY import_his_bm.sql     /app/import_his_bm.sql
#COPY import_bm_bv.sql      /app/import_bm_bv.sql
#COPY import.sh             /app/import.sh
#COPY docker-wait.sh        /app/docker-wait.sh


CMD /app/docker-entrypoint.sh

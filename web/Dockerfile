FROM amsterdam/python
MAINTAINER datapunt@amsterdam.nl

ENV PYTHONUNBUFFERED 1

EXPOSE 8000

RUN apt-get install --fix-missing -y libgeos-dev && \
        apt-get install -y netcat && \
       apt-get install -y postgis && \
	adduser --system datapunt && \
	mkdir -p /static && \
	mkdir -p /data && \
	mkdir -p /unzipped && \
	chown datapunt /static && \
	chown datapunt /data && \
	chown datapunt /unzipped && \
	pip install uwsgi

WORKDIR /app

COPY requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

COPY src /app/
COPY deploy /deploy/

USER datapunt

RUN export DJANGO_SETTINGS_MODULE=parkeervakken_api.settings

CMD uwsgi


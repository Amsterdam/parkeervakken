FROM amsterdam/docker_python:latest
MAINTAINER datapunt@amsterdam.nl

ENV PYTHONUNBUFFERED 1

EXPOSE 8000

RUN apt-get install --fix-missing -y libgeos-dev && \
        apt-get install -y netcat && \
	adduser --system datapunt && \
	mkdir -p /static && \
	chown datapunt /static && \
	pip install uwsgi

WORKDIR /app

COPY requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

USER datapunt
COPY parkeervakken_api /app/
CMD /app/docker-entrypoint.sh


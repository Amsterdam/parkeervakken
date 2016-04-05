FROM python:3.5
MAINTAINER datapunt@amsterdam.nl

ENV PYTHONUNBUFFERED 1

EXPOSE 8000

RUN apt-get update && \
	apt-get install -y libgeos-dev && \
	adduser --system datapunt && \
	mkdir -p /static && \
	chown datapunt /static && \
	pip install uwsgi

WORKDIR /app
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

USER datapunt
COPY atlas_milieuthemas /app/
CMD /app/docker-entrypoint.sh


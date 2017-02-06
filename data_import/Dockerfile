FROM python:3.5
MAINTAINER datapunt.ois@amsterdam.nl

ENV PYTHONUNBUFFERED 1


RUN apt-get update && apt-get install -y \
	libgeos-dev \
        postgis \
	netcat \
	unzip

RUN adduser --system datapunt && \
	mkdir -p /static && \
	mkdir -p /unzipped && \
	mkdir -p /unzipped/nietfiscaal && \
	chown datapunt /static && \
	pip install uwsgi


WORKDIR /app

COPY requirements.txt      /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py                  /app/
COPY *.sh                  /app/
COPY *.sql                 /app/
COPY .jenkins-import /.jenkins-import/

CMD /app/docker-entrypoint.sh

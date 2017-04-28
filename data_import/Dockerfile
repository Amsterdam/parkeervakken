FROM amsterdam/docker_python:latest
MAINTAINER datapunt@amsterdam.nl

ENV PYTHONUNBUFFERED 1

RUN apt-get install -y \
	postgresql-client \
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

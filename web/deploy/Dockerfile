ARG ENVIRONMENT
FROM build.datapunt.amsterdam.nl:5000/datapunt/parkeervakken:$ENVIRONMENT

USER root

RUN apt-get install -y postgis && \
	mkdir -p /data && \
	mkdir -p /unzipped && \
	chown datapunt /data && \
	chown datapunt /unzipped

WORKDIR /app

COPY import-requirements.txt /app/
RUN pip install --no-cache-dir -r import-requirements.txt

USER datapunt

COPY . /deploy/
CMD /deploy/import.sh
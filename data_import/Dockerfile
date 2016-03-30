FROM python:3.5

COPY import_data.py /import_data.py
COPY create_his_tables.sql /create_his_tables.sql
COPY create_bm_tables.sql /create_bm_tables.sql
COPY create_bv_tables.sql /create_bv_tables.sql
COPY update_his.sql /update_his.sql
COPY import_his_bm.sql /import_his_bm.sql
COPY import_bm_bv.sql /import_bm_bv.sql
COPY import.sh /import.sh
COPY requirements.txt /requirements.txt

RUN chmod +x /import.sh

RUN pip3.5 install -r /requirements.txt \
    && rm /requirements.txt

RUN apt-get update \
    && apt-get install -y postgis

CMD ["/import.sh"]

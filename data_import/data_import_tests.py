from pathlib import Path
from unittest import TestCase
import subprocess
from unittest.mock import MagicMock
import datetime

import psycopg2

import data_import


shape_script = b"""SET CLIENT_ENCODING TO UTF8;
SET STANDARD_CONFORMING_STRINGS TO ON;
BEGIN;
CREATE TABLE "public"."s_parkeervakken" (gid serial,
"parkeer_id" varchar(10),
"buurtcode" varchar(20),
"straatnaam" varchar(40),
"soort" varchar(10),
"type" varchar(20),
"aantal" numeric(10,0),
"kenteken" varchar(20),
"e_type" varchar(5),
"bord" varchar(50),
"begintijd1" varchar(20),
"eindtijd1" varchar(20),
"ma_vr" boolean,
"ma_za" boolean,
"zo" boolean,
"ma" boolean,
"di" boolean,
"wo" boolean,
"do" boolean,
"vr" boolean,
"za" boolean,
"eindtijd2" varchar(20),
"begintijd2" varchar(20),
"opmerking" varchar(100),
"tvm_begind" date,
"tvm_eindd" date,
"tvm_begint" varchar(20),
"tvm_eindt" varchar(20),
"tvm_opmerk" varchar(100));
ALTER TABLE "public"."s_parkeervakken" ADD PRIMARY KEY (gid);
SELECT AddGeometryColumn('public','s_parkeervakken','geom','0','MULTIPOLYGON',2);
INSERT INTO "public"."s_parkeervakken" ("parkeer_id","buurtcode","straatnaam","soort","type","aantal","kenteken","e_type","bord","begintijd1","eindtijd1","ma_vr","ma_za","zo","ma","di","wo","do","vr","za","eindtijd2","begintijd2","opmerking","tvm_begind","tvm_eindd","tvm_begint","tvm_eindt","tvm_opmerk",geom) VALUES ('A1234','','','','','1',NULL,NULL,NULL,NULL,NULL,'f','f','f','f','f','f','f','f','f',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'01060000000100000001030000000100000005000000713D0AD733D7FC401F85EB5197821D4148E17A142ED7FC409A999999AB821D410AD7A3704DD7FC40AE47E17AB9821D41E17A14AE57D7FC40A4703D0A98821D41713D0AD733D7FC401F85EB5197821D41');
INSERT INTO "public"."s_parkeervakken" ("parkeer_id","buurtcode","straatnaam","soort","type","aantal","kenteken","e_type","bord","begintijd1","eindtijd1","ma_vr","ma_za","zo","ma","di","wo","do","vr","za","eindtijd2","begintijd2","opmerking","tvm_begind","tvm_eindd","tvm_begint","tvm_eindt","tvm_opmerk",geom) VALUES ('A1235','','','','','1',NULL,NULL,NULL,NULL,NULL,'f','f','f','f','f','f','f','f','f',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'01060000000100000001030000000100000007000000E17A14AE57D7FC40A4703D0A98821D41CDCCCCCC5CD7FC4048E17A1487821D41666666665ED7FC400000000082821D418FC2F5283CD7FC401F85EB5181821D41A4703D0A37D7FC408FC2F5288C821D41713D0AD733D7FC401F85EB5197821D41E17A14AE57D7FC40A4703D0A98821D41');
CREATE INDEX ON "public"."s_parkeervakken" USING GIST ("geom");
COMMIT;
ANALYZE "public"."s_parkeervakken";"""


class TestImport(TestCase):
    def setUp(self):
        database = 'test_parkeervakken2'
        user = 'test'
        password = 'test'
        host = 'localhost'
        port = 5432

        conn = psycopg2.connect(database=database,
                                user=user,
                                password=password,
                                host=host,
                                port=port)

        cur = conn.cursor()

        self.conn = conn
        self.cur = cur

        subprocess.check_output = MagicMock(return_value=shape_script)

        drop_table = """DROP TABLE IF EXISTS public.h_parkeervakken CASCADE;"""

        create_table = """CREATE TABLE IF NOT EXISTS public.h_parkeervakken (
        "parkeer_id" varchar(10),
        "buurtcode" varchar(20),
        "straatnaam" varchar(40),
        "soort" varchar(20),
        "type" varchar(20),
        "aantal" numeric(10,0),
        "kenteken" varchar(20),
        "e_type" varchar(5),
        "bord" varchar(50),
        "begintijd1" varchar(20),
        "eindtijd1" varchar(20),
        "ma_vr" boolean,
        "ma_za" boolean,
        "zo" boolean,
        "ma" boolean,
        "di" boolean,
        "wo" boolean,
        "do" boolean,
        "vr" boolean,
        "za" boolean,
        "eindtijd2" varchar(20),
        "begintijd2" varchar(20),
        "opmerking" varchar(100),
        "tvm_begind" date,
        "tvm_eindd" date,
        "tvm_begint" varchar(20),
        "tvm_eindt" varchar(20),
        "tvm_opmerk" varchar(100)
        );
        SELECT AddGeometryColumn(%(schema)s,%(table)s,'geom','0','MULTIPOLYGON',2);
        ALTER TABLE "public"."h_parkeervakken"
            ADD COLUMN "stadsdeel" varchar(40),
            ADD COLUMN "goedkeurings_datum" timestamp without time zone;
        ALTER TABLE "public"."h_parkeervakken"
            ADD PRIMARY KEY (parkeer_id, goedkeurings_datum);"""

        params = {
            'schema': 'public',
            'table': 'h_parkeervakken',
        }

        try:
            cur.execute(drop_table)
            conn.commit()
            cur.execute(create_table, params)
            conn.commit()
        except Exception:
            conn.rollback()
            conn.close()
            raise

        # TODO create tables

    def test_create_partitioning(self):
        table = 'h_parkeervakken'
        schema = 'public'
        stadsdeel = 'test'

        data_import.create_partition(self.conn,
                                     self.cur,
                                     table,
                                     schema,
                                     stadsdeel)

        query = """SELECT *
        FROM pg_tables
        WHERE schemaname = 'public' AND tablename = 'h_parkeervakken_test'"""

        try:
            self.cur.execute(query)
            results = self.cur.fetchall()
        except Exception:
            self.conn.rollback()
            self.conn.close()
            raise

        self.assertEqual(len(results), 1)

    def test_import(self):
        data_import.load_shape_file(self.conn,
                                    self.cur,
                                    's_parkeervakken',
                                    'h_parkeervakken',
                                    'public',
                                    Path('test_parkeerhaven_RD_20150202.shp'))

        data_import.load_shape_file(self.conn,
                                    self.cur,
                                    's_parkeervakken',
                                    'h_parkeervakken',
                                    'public',
                                    Path('test_parkeerhaven_RD_20150203.shp'))

        query = """SELECT
            parkeer_id,
            stadsdeel,
            goedkeurings_datum
        FROM h_parkeervakken"""

        partition_query = """SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema = %(schema)s AND
              table_name = %(table)s;"""

        # Test if partition is created
        # Test if data is loaded

        try:
            self.cur.execute(query)
            results = self.cur.fetchall()
        except Exception:
            self.conn.rollback()
            self.conn.close()
            raise

        self.assertEqual(len(results), 4)

        ids = set([
            row[0]
            for row in results
        ])

        for i in ('A1234', 'A1235'):
            self.assertIn(i, ids)

        dates = set([
            row[2]
            for row in results
        ])

        print(dates)

        expected_dates = (
            datetime.datetime(2015, 2, 2),
            datetime.datetime(2015, 2, 3),
        )

        for date in expected_dates:
            self.assertIn(date, dates)

        stadsdelen = set([
            row[1]
            for row in results
        ])

        for stadsdeel in ('test', ):
            self.assertIn(stadsdeel, stadsdelen)

        try:
            params = {
                'schema': 'public',
                'table': 'h_parkeervakken_test',
            }
            self.cur.execute(partition_query, params)
            results = self.cur.fetchall()
        except Exception:
            self.conn.rollback()
            self.conn.close()
            raise

        self.assertEqual(len(results), 1)

    def tearDown(self):
        self.conn.close()

        del subprocess.check_output

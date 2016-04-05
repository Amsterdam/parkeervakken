-- Create history layer

CREATE SCHEMA IF NOT EXISTS his;

DROP TABLE IF EXISTS his.parkeervakken CASCADE;

CREATE TABLE his.parkeervakken (
    "parkeervak_id_md5" text PRIMARY KEY,
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

SELECT AddGeometryColumn('his','parkeervakken','geom','0','MULTIPOLYGON',2);

ALTER TABLE "his"."parkeervakken"
    ADD COLUMN "stadsdeel" varchar(40),
    ADD COLUMN "goedkeurings_datum" date;

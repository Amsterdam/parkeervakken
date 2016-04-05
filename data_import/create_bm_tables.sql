-- Create bussiness model layer

CREATE SCHEMA IF NOT EXISTS bm;

DROP TABLE IF EXISTS bm.parkeervakken CASCADE;

CREATE TABLE IF NOT EXISTS bm.parkeervakken (
    "parkeer_id_md5" text PRIMARY KEY,
    "parkeer_id" varchar(10),
    "stadsdeel" varchar(20),
    "buurtcode" varchar(20),
    "straatnaam" varchar(40),
    "type" varchar(20),
    "aantal" numeric(10,0),
    "e_type" varchar(5),
    "bord" varchar(50)

);

SELECT AddGeometryColumn('bm','parkeervakken','geom','0','MULTIPOLYGON',2);

DROP TABLE IF EXISTS bm.reserveringen_fiscaal CASCADE;

CREATE TABLE IF NOT EXISTS bm.reserveringen_fiscaal (
    "reserverings_key_md5" text PRIMARY KEY,
    "parkeer_id" varchar(10),
    "parkeer_id_md5" text,
    "soort" varchar(20),
    "kenteken" varchar(20) DEFAULT NULL,
    "reserverings_datum" date,
    "begin_datum" date,
    "eind_datum" date,
    "begin_tijd" time without time zone,
    "eind_tijd" time without time zone,
    "opmerkingen" varchar(100)
);

DROP TABLE IF EXISTS bm.reserveringen_mulder CASCADE;

CREATE TABLE IF NOT EXISTS bm.reserveringen_mulder (
    "reserverings_key_md5" text PRIMARY KEY,
    "parkeer_id" varchar(10),
    "parkeer_id_md5" text,
    "soort" varchar(20),
    "kenteken" varchar(20) DEFAULT NULL,

    "reserverings_datum" date,
    "begin_datum" date DEFAULT NULL,
    "eind_datum" date DEFAULT NULL,
    "begin_tijd" time without time zone,
    "eind_tijd" time without time zone,
    "opmerkingen" varchar(100)
);

DROP TABLE IF EXISTS bm.reserveringen_mulder_schoon CASCADE;

CREATE TABLE IF NOT EXISTS bm.reserveringen_mulder_schoon (
    "reserverings_key_md5" text PRIMARY KEY,
    "parkeer_id" varchar(10),
    "parkeer_id_md5" text,
    "soort" varchar(20),
    "kenteken" varchar(20) DEFAULT NULL,

    "reserverings_datum" date,
    "begin_datum" date DEFAULT NULL,
    "eind_datum" date DEFAULT NULL,
    "begin_tijd" time without time zone,
    "eind_tijd" time without time zone,
    "opmerkingen" varchar(100)
);

DROP TABLE IF EXISTS bm.datums CASCADE;

CREATE TABLE IF NOT EXISTS bm.datums (
    "datum" date,
    "dag" integer
);

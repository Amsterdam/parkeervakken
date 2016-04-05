-- Create bussiness view layer

CREATE SCHEMA IF NOT EXISTS bv;

DROP TABLE IF EXISTS bv.reserveringen CASCADE;

DROP TABLE IF EXISTS bv.parkeervakken CASCADE;

CREATE TABLE bv.parkeervakken (
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

SELECT AddGeometryColumn('bv','parkeervakken','geom','0','MULTIPOLYGON',2);

CREATE TABLE IF NOT EXISTS bv.reserveringen (
    "reserverings_key_md5" text PRIMARY KEY,
    "parkeer_id_md5" text,
    "parkeer_id" varchar(10),
    "soort" varchar(20),
    "kenteken" varchar(20) DEFAULT NULL,

    "reserverings_datum" date,
    "begin_datum" date DEFAULT NULL,
    "eind_datum" date DEFAULT NULL,
    "begin_tijd" time without time zone,
    "eind_tijd" time without time zone,
    "opmerkingen" varchar(100)
);

ALTER TABLE bv.reserveringen
    ADD CONSTRAINT fk_reserveringen
        FOREIGN KEY (parkeer_id_md5)
        REFERENCES bv.parkeervakken (parkeer_id_md5);

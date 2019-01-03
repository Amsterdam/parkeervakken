-- Create bussiness view layer

CREATE SCHEMA IF NOT EXISTS bv;

DROP TABLE IF EXISTS bv.reserveringen CASCADE;

DROP TABLE IF EXISTS bv.parkeervakken CASCADE;

DROP TABLE IF EXISTS bv.e_types CASCADE;

CREATE TABLE bv.e_types (
  "code" varchar(5) PRIMARY KEY,
  "title" text,
  "description" text
);

INSERT INTO bv.e_types(title, description, code) VALUES
('Parkeerverbod', 'Parkeerverbod','E1'),
('Verbod stil te staan', 'Verbod stil te staan', 'E2'),
('Verbod (brom)fietsen te plaatsen', 'Verbod fietsen en bromfietsen te plaatsen', 'E3'),
('Parkeergelegenheid', 'Parkeergelegenheid', 'E4'),
('Taxistandplaats', 'Taxistandplaats','E5'),
('Gehandicaptenparkeerplaats', 'Gehandicaptenparkeerplaats', 'E6'),
('Gehandicaptenparkeerplaats algemeen','Gehandicaptenparkeerplaats algemeen', 'E6a'),
('Gehandicaptenparkeerplaats op kenteken','Gehandicaptenparkeerplaats op kenteken', 'E6b'),
('Laden en lossen', 'Gelegenheid bestemd voor het onmiddellijk laden en lossen van goederen', 'E7'),
('Specifieke voertuigcategorie', 'Parkeergelegenheid alleen bestemd voor de voertuigcategorie of groep voertuigen die op het bord is aangegeven', 'E8'),
('Vergunninghouders', 'Parkeergelegenheid alleen bestemd voor vergunninghouders', 'E9'),
('Blauwe zone', 'Parkeerschijf-zone met verplicht gebruik van parkeerschijf, tevens parkeerverbod indien er langer wordt geparkeerd dan de parkeerduur die op het bord is aangegeven', 'E10'),
('Einde blauwe zone', 'Einde parkeerschijf-zone met verplicht gebruik van parkeerschijf', 'E11'),
('Park & Ride', 'Parkeergelegenheid ten behoeve van overstappers op het openbaar vervoer', 'E12'),
('Carpool', 'Parkeergelegenheid ten behoeve van carpoolers', 'E13');

CREATE TABLE bv.parkeervakken (
    "parkeer_id_md5" text PRIMARY KEY,
    "parkeer_id" varchar(30),
    "parkeer_geo_id" varchar(30),
    "stadsdeel" varchar(20),
    "buurtcode" varchar(20),
    "straatnaam" varchar(40),
    "soort" varchar(20),
    "type" varchar(20),
    "aantal" numeric(10,0),

    "e_type" varchar(5),
    "bord" varchar(50)

);

SELECT AddGeometryColumn('bv','parkeervakken','geom','0','MULTIPOLYGON',2);
SELECT AddGeometryColumn('bv','parkeervakken','geo_id','0','POINT',2);

CREATE TABLE IF NOT EXISTS bv.reserveringen (
    "reserverings_key_md5" text PRIMARY KEY,
    "parkeer_id_md5" text,
    "parkeer_id" varchar(30),
    "soort" varchar(20),
    "kenteken" varchar(20) DEFAULT NULL,

    "reserverings_datum" date,
    "begin_datum" date DEFAULT NULL,
    "eind_datum" date DEFAULT NULL,
    "begin_tijd" time without time zone,
    "eind_tijd" time without time zone,
    "opmerkingen" varchar(100),
    "reservering_bron" varchar(100)
);

ALTER TABLE bv.reserveringen
    ADD CONSTRAINT fk_reserveringen
        FOREIGN KEY (parkeer_id_md5)
        REFERENCES bv.parkeervakken (parkeer_id_md5);

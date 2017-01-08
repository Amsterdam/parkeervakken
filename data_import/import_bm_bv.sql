ALTER TABLE bv.reserveringen
DROP CONSTRAINT IF EXISTS fk_reserveringen;

TRUNCATE bv.parkeervakken;

INSERT INTO bv.parkeervakken
(
    parkeer_id_md5,
    parkeer_id,
    parkeer_geo_id,
    stadsdeel,
    buurtcode,
    straatnaam,
    soort,
    "type",
    aantal,

    e_type,
    bord,

    geom,
    geo_id
)
SELECT
    parkeer_id_md5,
    parkeer_id,
    concat(
        ST_X(ST_Centroid(geom))::integer,
        ST_Y(ST_Centroid(geom))::integer
    ),

    stadsdeel,
    buurtcode,
    straatnaam,
    soort,
    "type",
    aantal,

    e_type,
    bord,

    geom,
    ST_Centroid(geom) as geo_id

FROM bm.parkeervakken;

TRUNCATE bv.reserveringen;

INSERT INTO bv.reserveringen
(
    reserverings_key_md5,
    parkeer_id,
    parkeer_id_md5,
    soort,
    kenteken,

    reserverings_datum,
    begin_datum,
    eind_datum,
    begin_tijd,
    eind_tijd,
    opmerkingen
)
SELECT /* DISTINCT on (reserverings_key_md5) */
    *
FROM bm.reserveringen_fiscaal;

INSERT INTO bv.reserveringen
(
    reserverings_key_md5,
    parkeer_id,
    parkeer_id_md5,
    soort,
    kenteken,

    reserverings_datum,
    begin_datum,
    eind_datum,
    begin_tijd,
    eind_tijd,
    opmerkingen
)
SELECT /* DISTINCT ON (reserverings_key_md5) */
    *
FROM bm.reserveringen_mulder_schoon;

ALTER TABLE bv.reserveringen
    ADD CONSTRAINT fk_reserveringen
    FOREIGN KEY (parkeer_id_md5)
    REFERENCES bv.parkeervakken
        (parkeer_id_md5);

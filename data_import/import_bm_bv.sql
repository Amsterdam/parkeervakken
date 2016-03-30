ALTER TABLE bv.reserveringen
DROP CONSTRAINT IF EXISTS fk_reserveringen;

TRUNCATE bv.parkeervakken;

INSERT INTO bv.parkeervakken
(
    parkeer_id_md5,
    parkeer_id,
    stadsdeel,
    buurtcode,
    straatnaam,
    "type",
    aantal,
    geom
)
SELECT
    *
FROM bm.parkeervakken;

TRUNCATE bv.reserveringen;

INSERT INTO bv.reserveringen
(
    reserverings_key_md5,
    parkeer_id,
    parkeer_id_md5,
    soort,
    kenteken,
    e_type,
    bord,
    reserverings_datum,
    begin_datum,
    eind_datum,
    begin_tijd,
    eind_tijd,
    opmerkingen
)
SELECT
    *
FROM bm.reserveringen_fiscaal;

INSERT INTO bv.reserveringen
(
    reserverings_key_md5,
    parkeer_id,
    parkeer_id_md5,
    soort,
    kenteken,
    e_type,
    bord,
    reserverings_datum,
    begin_datum,
    eind_datum,
    begin_tijd,
    eind_tijd,
    opmerkingen
)
SELECT
    *
FROM bm.reserveringen_mulder_schoon;

ALTER TABLE bv.reserveringen
    ADD CONSTRAINT fk_reserveringen
    FOREIGN KEY (parkeer_id_md5)
    REFERENCES bv.parkeervakken
        (parkeer_id_md5);
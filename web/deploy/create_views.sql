SELECT UpdateGeometrySRID('bv', 'parkeervakken', 'geom', 28992);

CREATE VIEW public.geo_parkeervakken AS

  SELECT
    pv.parkeer_id      AS id,
    pv.parkeer_geo_id  AS geo_id,
    pv.buurtcode       AS buurtcode,
    pv.stadsdeel       AS stadsdeel,
    pv.straatnaam      AS straatnaam,
    pv.aantal          AS aantal,
    pv.soort           AS soort,
    pv.type            AS type,

    pv.e_type          AS e_type,
    (CASE WHEN pv.e_type = 'E6b'
      THEN NULL
     ELSE pv.bord END) AS bord,

    pv.geom            AS geometrie
  FROM bv.parkeervakken pv;


CREATE VIEW bv.geo_parkeervakken AS

  SELECT
    pv.parkeer_id      AS id,
    pv.parkeer_geo_id  AS geo_id,
    pv.buurtcode       AS buurtcode,
    pv.stadsdeel       AS stadsdeel,
    pv.straatnaam      AS straatnaam,
    pv.aantal          AS aantal,
    pv.soort           AS soort,
    pv.type            AS type,

    pv.e_type          AS e_type,
    (CASE WHEN pv.e_type = 'E6b'
      THEN NULL
     ELSE pv.bord END) AS bord,

    pv.geom            AS geometrie
  FROM bv.parkeervakken pv;

CREATE VIEW bv.geo_parkeervakken_reserveringen AS

  SELECT
    pv.parkeer_id         AS id,
    pv.buurtcode          AS buurtcode,
    pv.stadsdeel          AS stadsdeel,
    pv.straatnaam         AS straatnaam,
    pv.aantal             AS aantal,
    pv.geom               AS geometrie,

    pv.e_type             AS e_type,
    (CASE WHEN pv.e_type = 'E6b'
      THEN NULL
     ELSE pv.bord END)    AS bord,

    re.begin_datum        AS begin_datum,
    re.eind_datum         AS eind_datum,
    re.soort              AS soort,
    re.opmerkingen        AS opmerkingen,
    re.reserverings_datum AS day

  FROM bv.parkeervakken pv
    LEFT JOIN bv.reserveringen re
      ON pv.parkeer_id_md5 = re.parkeer_id_md5 AND reserverings_datum = current_date;

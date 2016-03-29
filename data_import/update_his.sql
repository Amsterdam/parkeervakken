UPDATE his.parkeervakken
SET
    parkeervak_id_md5 = md5(concat(
        a.parkeer_id,
        '-',
        a.tvm_begind,
        '-',
        a.tvm_begint)),
    begintijd1 = a.begintijd1,
    eindtijd1 = a.eindtijd1,
    begintijd2 = a.begintijd2,
    eindtijd2 = a.eindtijd2,
    tvm_begint = a.tvm_begint,
    tvm_eindt = a.tvm_eindt
FROM (
    SELECT
        parkeer_id,
        parkeervak_id_md5,
        CASE begintijd1 ~ '^[0-9][0-9]?[:;][0-9][0-9]?([:;][0-9][0-9]?)?$'
            WHEN TRUE THEN begintijd1
            ELSE NULL
        END AS begintijd1,
        CASE eindtijd1 ~ '^[0-9][0-9]?[:;][0-9][0-9]?([:;][0-9][0-9]?)?$'
            WHEN TRUE THEN eindtijd1
            ELSE NULL
        END AS eindtijd1,
        CASE begintijd2 ~ '^[0-9][0-9]?[:;][0-9][0-9]?([:;][0-9][0-9]?)?$'
            WHEN TRUE THEN begintijd2
            ELSE NULL
        END AS begintijd2,
        CASE eindtijd2 ~ '^[0-9][0-9]?[:;][0-9][0-9]?([:;][0-9][0-9]?)?$'
            WHEN TRUE THEN eindtijd2
            ELSE NULL
        END AS eindtijd2,
        CASE tvm_begint ~ '^[0-9][0-9]?[:;][0-9][0-9]?([:;][0-9][0-9]?)?$'
            WHEN TRUE THEN tvm_begint
            ELSE NULL
        END AS tvm_begint,
        CASE tvm_eindt ~ '^[0-9][0-9]?[:;][0-9][0-9]?([:;][0-9][0-9]?)?$'
            WHEN TRUE THEN tvm_eindt
            ELSE NULL
        END AS tvm_eindt
    FROM his.parkeervakken
) AS a
WHERE his.parkeervakken.parkeervak_id_md5 = a.parkeervak_id_md5;

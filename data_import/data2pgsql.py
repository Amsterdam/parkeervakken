import subprocess
import datetime
import argparse
import re
import zipfile
import shutil
import shlex
import pathlib

import psycopg2


# The way file names are expected to be.
stadsdeel_c = re.compile(r'^(?P<stadsdeel>[a-zA-Z-]*)_parkeerhaven.*_'
                         r'(?P<date>[0-9]{8})$')


def setup_argparse():
    """Setup argument parser for command line options. It also includes a
    subparser to differentiate between initializing the tables in the database
    and importing data from shape files.
    """

    parser_description = """Either setup tables in a database for loading shape
    files or load data into the tables."""

    parser = argparse.ArgumentParser(description=parser_description)

    parser.add_argument('--schema',
                        dest='schema',
                        default='public',
                        required=False,
                        help="The schema name where all the tables are found")

    parser.add_argument('--parking-table',
                        dest='park_table',
                        default='parkeervakken',
                        required=False)

    parser.add_argument('--reservation-table',
                        dest='res_table',
                        default='reserveringen',
                        required=False)

    parser.add_argument('--history-table',
                        dest='history_table',
                        default='h_parkeervakken',
                        required=False,
                        help=("This is the name of the table to which data "
                              "from the connection layer is written to. It is "
                              "a history table, containing the data from "
                              "shape files."))

    parser.add_argument('--temporary-table',
                        dest='temporary_table',
                        default='s_parkeervakken',
                        required=False,
                        help=("This is the name of the table in the "
                              "connection layer for data read from shape "
                              "files. This table is temporary and is cleaned "
                              "before a new file is imported."))

    parser.add_argument('--date-table',
                        dest='date_table',
                        default='datums',
                        required=False)

    parser.add_argument('--database', '-db',
                        dest='database',
                        required=True,
                        help='The name of the database')

    parser.add_argument('--user', '-U',
                        dest='user',
                        help='The database username')

    parser.add_argument('--password', '-W',
                        dest='password',
                        help='The password of the database user')

    parser.add_argument('--host', '-H',
                        dest='host',
                        default='localhost',
                        required=False,
                        help='The host where the database is resided')

    parser.add_argument('--port', '-P',
                        dest='port',
                        default=5432,
                        type=int,
                        required=False,
                        help='The port of the database')

    subparsers = parser.add_subparsers()

    init_description = ('Initialize the tables in a database. The structure '
                        'consists of 2 layers, the table in the first layer,'
                        'is dropped for each new shape file that is loaded. '
                        'The second layer is a history layer containing 1 '
                        'table. This contains data from all the loaded shape '
                        'files. This table is partioned on "stadsdeel".')

    init_parser = subparsers.add_parser('initialize', help=init_description)
    init_parser.add_argument('--force-drop',
                             action='store_true',
                             default=False,
                             help="Drop tables if they already exist")
    init_parser.set_defaults(command='init')

    update_parser = subparsers.add_parser('update')
    update_parser.set_defaults(command='update')

    update_parser.add_argument('--source',
                               dest='source',
                               type=pathlib.Path,
                               required=True,
                               help='The path where zip-files are resided')

    update_parser.add_argument('--target',
                               dest='target',
                               type=pathlib.Path,
                               default=None,
                               required=False,
                               help=("The target directory to where zip files "
                                     "should be unzipped. If the target is "
                                     "not given or the path is the same as "
                                     "the target path, it will be changed to "
                                     "<source_path>/tmp"))

    update_parser.add_argument('--skip-import',
                               dest='skip_import',
                               default=False,
                               action='store_true')

    update_parser.add_argument('--skip-dates',
                               dest='skip_dates',
                               default=False,
                               action='store_true')

    update_parser.add_argument('--skip-parking',
                               dest='skip_parking',
                               default=False,
                               action='store_true')

    update_parser.add_argument('--skip-reservations',
                               dest='skip_reservations',
                               default=False,
                               action='store_true')
    return parser.parse_args()


def clean_directory(directory):
    """Remove all data from a directory, if it doesn't exist do nothing.

    :type directory: pathlib.Path
    """

    if not directory.exists():
        return

    shutil.rmtree(str(directory))


def import_data(database,
                user,
                password,
                host,
                port,
                source,
                target=None,
                schema='public',
                hist_table='h_parkeervakken',
                tmp_table='s_parkeervakken',
                park_table='parkeervakken',
                res_table='reserveringen',
                date_table='datums',
                skip_import=False,
                skip_dates=False,
                skip_parking=False,
                skip_reservations=False):
    """Load data from zip files given in :param:`source` into the database.
    These zip files should consist of shape files and files connected to the
    shape files. The data in shape files contains data about reservations of
    parking spaces. There should be files for each `stadsdeel` (city part), but
    it doesn't matter if a `stadsdeel` is ommitted.

    :type database: str
    :type user: str
    :type password: str
    :type host: str
    :type port: int
    :type source: pathlib.Path
    :type target: pathlib.Path
    :type schema: str
    :type hist_table: str
    :type tmp_table: str
    :type park_table: str
    :type res_table: str
    :type date_table: str
    :type skip_import: bool
    :type skip_dates: bool
    :type skip_parking: bool
    :type skip_reservations: bool
    """

    source = source.absolute()

    if target is None or str(target.absolute()) == str(source):
        target = source / 'tmp'

    target = target.absolute()

    conn = psycopg2.connect(database=database,
                            user=user,
                            password=password,
                            host=host,
                            port=port)

    cur = conn.cursor()

    if not skip_import:
        for f in source.iterdir():
            if f.suffix != '.zip':
                continue

            import_zip_data(f,
                            conn,
                            cur,
                            target,
                            schema,
                            hist_table,
                            tmp_table)

    if not skip_dates:
        update_dates(conn, cur, date_table, hist_table, schema)

    if not skip_parking:
        update_parking_spaces(conn, cur, hist_table, park_table, schema)

    if not skip_reservations:
        update_reservations(conn,
                            cur,
                            hist_table,
                            res_table,
                            date_table,
                            schema)
    conn.close()


def import_zip_data(zip_file,
                    conn,
                    cur,
                    target,
                    schema,
                    hist_table,
                    tmp_table):
    """Unzip all data in :param:`zip_file` and load the shape files.

    :type conn: psycopg2.extensions.connection
    :type cur: psycopg2.extensions.connection
    :type zip_file: pathlib.Path
    :param zip_file: The path to where the zip file is resided on disk.
    :type target: pathlib.Path
    :param target: The target path to which data from the zipfile should be
    unzipped. Note that the target directory is first cleaned so the target
    can't be the same as :param:`zip_file`. This is handled by changing the
    target to `<target>/tmp`.
    """

    if str(zip_file) == str(target):
        target = target / 'tmp'

    clean_directory(target)

    with zipfile.ZipFile(str(zip_file)) as z:
        z.extractall(str(target))

    for shp_file in target.glob('*.shp'):
        load_shape_file(conn, cur, tmp_table, hist_table, schema, shp_file)


def load_shape_file(conn, cur, tmp_table, hist_table, schema, shp_file):
    """Import data from shape files into the database. The shape files are
    imported by first creating an sql script using `shp2pgsql` and executing
    this on the database.

    :type conn: psycopg2.extensions.connection
    :type cur: psycopg2.extensions.connection
    :type tmp_table: str
    :param tmp_table: The table name to which the data from the shape files is
    imported. Note that this table is dropped and recreated before inserting
    any data.
    :type hist_table: str
    :param hist_table: The hostory table to which data is moved after it is
    imported. This table is used to the source for other tables and should
    contain all data ever imported.
    :type schema: str
    :param schema: The schema name where the tables are created in.
    :type shp_file: pathlib.Path
    :param shp_file: The path to the shape file.
    """

    match = stadsdeel_c.match(shp_file.stem)

    if match is None:
        conn.close()

        # TODO
        raise Exception

    stadsdeel, date_string = match.groups()

    try:
        date = datetime.datetime.strptime(date_string, '%Y%m%d')
    except Exception:
        date = None

    if stadsdeel == '':
        stadsdeel = 'unknown'

    stadsdeel = stadsdeel.lower().replace('-', '_')

    drop_table(conn, cur, tmp_table, schema)

    cmd_fmt = 'shp2pgsql -W LATIN1 -I {shape_file} {schema}.{table}'

    shp2pgsql_cmd = shlex.split(cmd_fmt.format(shape_file=str(shp_file),
                                               table=tmp_table,
                                               schema=schema))

    # Create the sql statements for loading shape data into the database
    try:
        output = subprocess.check_output(shp2pgsql_cmd)
        shp_stmts = output.decode()
    except Exception:
        conn.close()
        raise

    # Load shape file into temporary table in the database
    try:
        cur.execute(shp_stmts)
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise

    # Update data from the temporary table into the history table
    columns = (
        "parkeer_id",
        "buurtcode",
        "straatnaam",
        "soort",
        "type",
        "aantal",
        "kenteken",
        "e_type",
        "bord",
        "begintijd1",
        "eindtijd1",
        "ma_vr",
        "ma_za",
        "zo",
        "ma",
        "di",
        "wo",
        "do",
        "vr",
        "za",
        "eindtijd2",
        "begintijd2",
        "opmerking",
        "tvm_begind",
        "tvm_eindd",
        "tvm_begint",
        "tvm_eindt",
        "tvm_opmerk",
        "geom",
    )

    update_history(conn,
                   cur,
                   tmp_table,
                   hist_table,
                   schema,
                   stadsdeel,
                   date,
                   columns)


def update_history(conn,
                   cur,
                   tmp_table,
                   hist_table,
                   schema,
                   stadsdeel,
                   date,
                   columns='*'):
    """Copy data from :param:`tmp_table` to :param:`hist_table`. First all data
    with the same :param:`stadsdeel` and :param:`date` is deleted from
    :param:`hist_table`. After that new data is inserted.

    :type conn: psycopg2.extensions.connection
    :type cur: psycopg2.extensions.connection
    :type tmp_table: str
    :type hist_table: str
    :type schema: str
    :type stadsdeel: str
    :type date: str
    """

    if isinstance(columns, (list, tuple)):
        quoted = [
            '"{}"'.format(column)
            for column in columns
        ]
        columns = ', '.join(quoted)

    partition_table = create_partition(conn,
                                       cur,
                                       hist_table,
                                       schema,
                                       stadsdeel)

    # First delete data from the history table before inserting new data
    delete_stmt = """DELETE FROM {schema}.{hist_table}
    WHERE goedkeurings_datum = %(date)s""".format(schema=schema,
                                                  hist_table=partition_table)

    try:
        cur.execute(delete_stmt, {'date': date})
        conn.commit()
    except Exception:
        conn.close()
        raise

    # Insert data from the temporary table into the history table
    insert_stmt = """INSERT INTO {schema}.{hist_table}
    SELECT
        {columns},
        %(stadsdeel)s AS stadsdeel,
        %(date)s AS goedkeurings_datum
    FROM {schema}.{tmp_table}""".format(schema=schema,
                                        hist_table=partition_table,
                                        columns=columns,
                                        tmp_table=tmp_table)

    try:
        cur.execute(insert_stmt, {'stadsdeel': stadsdeel, 'date': date})
        conn.commit()
    except Exception:
        conn.close()
        raise


def update_dates(conn,
                 cur,
                 date_table,
                 hist_table,
                 schema,
                 interval='20 year'):
    """

    :type conn: psycopg2.extensions.connection
    :type cur: psycopg2.extensions.connection
    :type date_table: str
    :type hist_table: str
    :type schema: str
    """

    truncate = "TRUNCATE {schema}.{date_table}".format(schema=schema,
                                                       date_table=date_table)

    try:
        cur.execute(truncate)
        conn.commit()
    except Exception:
        conn.close()
        raise

    insert = """INSERT INTO {schema}.{date_table}
    SELECT datum, date_part('dow', datum)
    FROM (
        SELECT
            generate_series(min_date, max_date, interval '1d') as datum
        FROM (
            SELECT
                min(goedkeurings_datum) AS min_date,
                max(goedkeurings_datum) + interval %(interval)s AS max_date
            FROM {schema}.{hist_table}
        ) AS u
    ) as t
    """.format(schema=schema,
               date_table=date_table,
               hist_table=hist_table)

    try:
        cur.execute(insert, {'interval': interval})
        conn.commit()
    except Exception:
        conn.close()
        raise


def update_parking_spaces(conn,
                          cur,
                          hist_table,
                          park_table,
                          schema):
    """

    :type conn: psycopg2.extensions.connection
    :type cur: psycopg2.extensions.connection
    :type hist_table: str
    :type park_table: str
    :type schema: str
    """

    truncate = "TRUNCATE {schema}.{park_table}".format(schema=schema,
                                                       park_table=park_table)

    try:
        cur.execute(truncate)
        conn.commit()
    except Exception:
        conn.close()
        raise

    insert = """INSERT INTO {schema}.{park_table}
    SELECT
        t.parkeer_id,
        t.buurtcode,
        t.straatnaam,
        t."type",
        t.aantal,
        last_t.laatste_update,
        stadsdeel,
        geom
    FROM {schema}.{hist_table} as t
    INNER JOIN (
        SELECT
            parkeer_id,
            MAX(goedkeurings_datum) AS laatste_update
        FROM {schema}.{hist_table}
        GROUP BY parkeer_id
    ) AS last_t
        ON t.parkeer_id = last_t.parkeer_id AND
           t.goedkeurings_datum = last_t.laatste_update
    """.format(schema=schema, hist_table=hist_table, park_table=park_table)

    try:
        cur.execute(insert)
        conn.commit()
    except Exception:
        conn.close()
        raise


def update_fiscal_reservations(conn,
                               cur,
                               hist_table,
                               res_table,
                               date_table,
                               schema):
    """

    :type conn: psycopg2.extensions.connection
    :type cur: psycopg2.extensions.connection
    :type hist_table: str
    :type res_table: str
    :type date_table: str
    :type schema: str
    """

    insert = """INSERT INTO {schema}.{res_table}
    SELECT DISTINCT ON (parkeer_id, reserverings_datum, begintijd)
        parkeer_id,
        soort,
        dates.datum AS reserverings_datum,
        CASE WHEN tvm_begind = datum
                THEN begintijd
             ELSE '00:00:00'::time
        END AS begintijd,
        CASE WHEN tvm_eindd = datum
                THEN eindtijd
             ELSE '24:00:00'::time
        END AS eindtijd,
        NULL AS kenteken,
        NULL AS e_type,
        NULL AS bord,
        opmerkingen
    FROM {schema}.{date_table} AS dates
    INNER JOIN (
        SELECT
            parkeer_id,
            soort,
            CASE tvm_begint ~ '^[0-9][0-9]?[:;][0-9][0-9]?([:;][0-9][0-9]?)?$'
                WHEN TRUE THEN replace(tvm_begint, ';', ':')::time
                ELSE '00:00:00'::time
            END AS begintijd,
            CASE tvm_eindt ~ '^[0-9][0-9]?[:;][0-9][0-9]?([:;][0-9][0-9]?)?$'
                WHEN TRUE THEN replace(tvm_eindt, ';', ':')::time
                ELSE '00:00:00'::time
            END AS eindtijd,
            tvm_opmerk AS opmerkingen,
            tvm_begind,
            tvm_eindd
        FROM {schema}.{hist_table}
        WHERE tvm_begint ~ '^[0-9][0-9]?[:;][0-9][0-9]?([:;][0-9][0-9]?)?$' AND
              tvm_eindt ~ '^[0-9][0-9]?[:;][0-9][0-9]?([:;][0-9][0-9]?)?$' AND
              soort = 'FISCAAL' AND
              tvm_begind <= tvm_eindd
    ) AS reserverings_tijden
        ON reserverings_tijden.tvm_begind <= dates.datum AND
           dates.datum <= reserverings_tijden.tvm_eindd
    WHERE reserverings_tijden.tvm_begind != reserverings_tijden.tvm_eindd OR
          reserverings_tijden.begintijd <= reserverings_tijden.eindtijd
    """.format(schema=schema,
               hist_table=hist_table,
               res_table=res_table,
               date_table=date_table)

    try:
        cur.execute(insert)
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise


def update_mulder_reservations(conn,
                               cur,
                               hist_table,
                               res_table,
                               date_table,
                               schema):
    """

    :type conn: psycopg2.extensions.connection
    :type cur: psycopg2.extensions.connection
    :type hist_table: str
    :type res_table: str
    :type date_table: str
    :type schema: str
    """

    mulder_select_approval_dates = """SELECT
        parkeer_id,
        soort,
        CASE begintijd1 ~ '^[0-9][0-9]?[:;][0-9][0-9]?([:;][0-9][0-9]?)?$'
             WHEN TRUE THEN replace(begintijd1, ';', ':')::time
             ELSE '00:00:00'::time
        END AS begintijd,
        CASE eindtijd1 ~ '^[0-9][0-9]?[:;][0-9][0-9]?([:;][0-9][0-9]?)?$'
             WHEN TRUE THEN replace(eindtijd1, ';', ':')::time
             ELSE '00:00:00'::time
        END AS eindtijd,
        ma_vr,
        ma_za,
        zo,
        ma,
        di,
        wo,
        "do",
        vr,
        za,
        kenteken,
        e_type,
        bord,
        opmerking,
        max(goedkeurings_datum) as goedkeuring
    FROM {schema}.{hist_table}
    WHERE (begintijd1 ~ '^[0-9][0-9]?[:;][0-9][0-9]?([:;][0-9][0-9]?)?$' OR
           begintijd1 = '' OR begintijd1 IS NULL) AND
          (eindtijd1 ~ '^[0-9][0-9]?[:;][0-9][0-9]?([:;][0-9][0-9]?)?$' OR
           eindtijd1 = '' OR eindtijd1 IS NULL) AND
            soort = 'MULDER'
    GROUP BY
        parkeer_id,
        soort,
        begintijd1,
        eindtijd1,
        ma_vr,
        ma_za,
        zo,
        ma,
        di,
        wo,
        "do",
        vr,
        za,
        kenteken,
        e_type,
        bord,
        opmerking
    UNION ALL
    SELECT
        parkeer_id,
        soort,
        CASE begintijd2 ~ '^[0-9][0-9]?[:;][0-9][0-9]?([:;][0-9][0-9]?)?$'
             WHEN TRUE THEN replace(begintijd2, ';', ':')::time
             ELSE '00:00:00'::time
        END AS begintijd,
        CASE eindtijd2 ~ '^[0-9][0-9]?[:;][0-9][0-9]?([:;][0-9][0-9]?)?$'
             WHEN TRUE THEN replace(eindtijd2, ';', ':')::time
             ELSE '00:00:00'::time
        END AS eindtijd,
        ma_vr,
        ma_za,
        zo,
        ma,
        di,
        wo,
        "do",
        vr,
        za,
        kenteken,
        e_type,
        bord,
        opmerking,
        max(goedkeurings_datum) as goedkeuring
    FROM {schema}.{hist_table}
    WHERE begintijd2 ~ '^[0-9][0-9]?[:;][0-9][0-9]?([:;][0-9][0-9]?)?$' AND
          eindtijd2 ~ '^[0-9][0-9]?[:;][0-9][0-9]?([:;][0-9][0-9]?)?$' AND
          soort = 'MULDER'
    GROUP BY
        parkeer_id,
        soort,
        begintijd2,
        eindtijd2,
        ma_vr,
        ma_za,
        zo,
        ma,
        di,
        wo,
        "do",
        vr,
        za,
        kenteken,
        e_type,
        bord,
        opmerking
    """.format(schema=schema, hist_table=hist_table)

    mulder_insert = """INSERT INTO {schema}.{res_table}
    SELECT
        reserverings_tijden.parkeer_id,
        reserverings_tijden.soort,
        dates.datum,
        reserverings_tijden.begintijd,
        reserverings_tijden.eindtijd,
        reserverings_tijden.kenteken,
        reserverings_tijden.e_type,
        reserverings_tijden.bord,
        reserverings_tijden.opmerking
    FROM {schema}.{date_table} AS dates
    INNER JOIN (
        SELECT *
        FROM ({approval_dates}) as t, (
            SELECT max(goedkeurings_datum) AS laatste_goedkeuring
            FROM {schema}.{hist_table}
        ) AS s
        WHERE begintijd <= eindtijd
    ) AS reserverings_tijden
        ON ((reserverings_tijden.ma_vr AND dates.dag >= 1 AND dates.dag <= 5)
            OR
            (reserverings_tijden.ma_za AND dates.dag >= 1 AND dates.dag <= 6)
            OR
            (reserverings_tijden.zo AND dates.dag = 0)
            OR
            (reserverings_tijden.ma AND dates.dag = 1)
            OR
            (reserverings_tijden.di AND dates.dag = 2)
            OR
            (reserverings_tijden.wo AND dates.dag = 3)
            OR
            (reserverings_tijden."do" AND dates.dag = 4)
            OR
            (reserverings_tijden.vr AND dates.dag = 5)
            OR
            (reserverings_tijden.za AND dates.dag = 6)
            OR
            (NOT reserverings_tijden.ma_vr AND
             NOT reserverings_tijden.ma_za AND
             NOT reserverings_tijden.zo AND
             NOT reserverings_tijden.ma AND
             NOT reserverings_tijden.di AND
             NOT reserverings_tijden.wo AND
             NOT reserverings_tijden."do" AND
             NOT reserverings_tijden.vr AND
             NOT reserverings_tijden.za)
            ) AND
           laatste_goedkeuring <= goedkeuring AND
           dates.datum >= goedkeuring
    """.format(schema=schema,
               res_table=res_table,
               hist_table=hist_table,
               date_table=date_table,
               approval_dates=mulder_select_approval_dates)

    try:
        cur.execute(mulder_insert)
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise


def update_reservations(conn,
                        cur,
                        hist_table,
                        res_table,
                        date_table,
                        schema):
    """

    :type conn: psycopg2.extensions.connection
    :type cur: psycopg2.extensions.connection
    :type hist_table: str
    :type res_table: str
    :type date_table: str
    :type schema: str
    """

    truncate = "TRUNCATE {schema}.{res_table}".format(schema=schema,
                                                      res_table=res_table)

    try:
        cur.execute(truncate)
        conn.commit()
    except Exception:
        conn.close()
        raise

    update_fiscal_reservations(conn,
                               cur,
                               hist_table,
                               res_table,
                               date_table,
                               schema)

    update_mulder_reservations(conn,
                               cur,
                               hist_table,
                               res_table,
                               date_table,
                               schema)


def create_partition(conn, cur, table, schema, stadsdeel):
    """Create a partition for a :param:`stadsdeel` if it not already exists.

    :type conn: psycopg2.extensions.connection
    :type cur: psycopg2.extensions.connection
    :type table: str
    :param table: The table name on which a new partition should be created.
    :type schema: str
    :type stadsdeel: str
    :rtype: str
    """

    partition_table = (
        '{table}_{stadsdeel}'.format(table=table, stadsdeel=stadsdeel)
    )

    if table_exists(conn, cur, partition_table, schema):
        return partition_table

    stmt = """CREATE TABLE IF NOT EXISTS {schema}.{stadsdeel_table} (
        CHECK ( stadsdeel = %(stadsdeel)s )
    ) INHERITS ({schema}.{table})""".format(schema=schema,
                                            stadsdeel_table=partition_table,
                                            table=table)

    try:
        cur.execute(stmt, {'stadsdeel': stadsdeel})
    except Exception:
        conn.close()
        raise

    return partition_table


def drop_table(conn, cur, table, schema):
    """Drop a table.

    :type conn: psycopg2.extensions.connection
    :type cur: psycopg2.extensions.connection
    :type table: str
    :type schema: str
    """

    stmt = (
        "DROP TABLE IF EXISTS {schema}.{table} CASCADE"
        .format(schema=schema, table=table)
    )

    try:
        cur.execute(stmt)
        conn.commit()
    except Exception:
        conn.close()
        raise


def table_exists(conn, cur, table, schema):
    """Check if a table exists.

    :type conn: psycopg2.extensions.connection
    :type cur: psycopg2.extensions.connection
    :type table: str
    :type schema: str
    :rtype: bool
    """

    table_exists = """SELECT *
    FROM information_schema.tables
    WHERE table_schema = %(schema)s AND table_name = %(table)s"""

    try:
        cur.execute(table_exists, {'schema': schema, 'table': table})
        results = cur.fetchall()

    except Exception:
        conn.close()
        raise

    return len(results) > 0


def create_parking_space_table(conn, cur, table, schema, force_drop=False):
    """

    :type conn: psycopg2.extensions.connection
    :type cur: psycopg2.extensions.connection
    :type table: str
    :type schema: str
    :type force_drop: bool
    """

    if force_drop:
        drop_table(conn, cur, table, schema)
    elif table_exists(conn, cur, table, schema):
        return

    create_table = """CREATE TABLE IF NOT EXISTS {schema}.{table} (
    "parkeer_id" varchar(10),
    "buurtcode" varchar(20),
    "straatnaam" varchar(40),
    "type" varchar(20),
    "aantal" numeric(10,0),
    "laatste_update" timestamp without time zone,
    "stadsdeel" varchar(40),
    PRIMARY KEY (parkeer_id)
    );
    SELECT AddGeometryColumn(%(schema)s,%(table)s,'geom','0','MULTIPOLYGON',2);
    """.format(schema=schema, table=table)

    try:
        cur.execute(create_table, {'schema': schema, 'table': table})
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise


def create_parking_reservation_table(conn,
                                     cur,
                                     table,
                                     schema,
                                     force_drop=False):
    """

    :type conn: psycopg2.extensions.connection
    :type cur: psycopg2.extensions.connection
    :type table: str
    :type schema: str
    :type force_drop: bool
    """

    if force_drop:
        drop_table(conn, cur, table, schema)
    elif table_exists(conn, cur, table, schema):
        return

    create_table = """CREATE TABLE IF NOT EXISTS {schema}.{table} (
    "parkeer_id" varchar(10),
    "soort" varchar(20),
    "reserverings_datum" timestamp without time zone,
    "begintijd" time without time zone,
    "eindtijd" time without time zone,
    "kenteken" varchar(20),
    "e_type" varchar(5),
    "bord" varchar(50),
    "opmerkingen" varchar(100),
    PRIMARY KEY(parkeer_id, reserverings_datum, begintijd)
    );""".format(schema=schema, table=table)

    try:
        cur.execute(create_table, {'schema': schema, 'table': table})
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise


def create_hist_table(conn, cur, table, schema, force_drop=False):
    """Create the history table for shape files. This table should contain all
    data from all shape files that have been imported. The table is also
    partitioned according to `stadsdeel` and if a certain shapefile is imported
    with a stadsdeel and request data that is already imported, this data is
    deleted and the new shapefile is inserted.

    :type conn: psycopg2.extensions.connection
    :type cur: psycopg2.extensions.connection
    :type table: str
    :type schema: str
    :type force_drop: bool
    """

    if force_drop:
        drop_table(conn, cur, table, schema)
    elif table_exists(conn, cur, table, schema):
        return

    create_table = """CREATE TABLE IF NOT EXISTS {schema}.{table} (
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
    ALTER TABLE "{schema}"."{table}"
        ADD COLUMN "stadsdeel" varchar(40),
        ADD COLUMN "goedkeurings_datum" timestamp without time zone;
    ALTER TABLE "{schema}"."{table}"
        ADD PRIMARY KEY (parkeer_id, goedkeurings_datum);
    """.format(schema=schema, table=table)

    try:
        cur.execute(create_table, {'schema': schema, 'table': table})
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise


def create_date_table(conn, cur, table, schema, force_drop=False):
    """

    :type conn: psycopg2.extensions.connection
    :type cur: psycopg2.extensions.connection
    :type table: str
    :type schema: str
    :type force_drop: bool
    """

    if force_drop:
        drop_table(conn, cur, table, schema)
    elif table_exists(conn, cur, table, schema):
        return

    create_table = """CREATE TABLE IF NOT EXISTS {schema}.{table} (
        datum timestamp without time zone,
        dag int,
        PRIMARY KEY (datum, dag)
    )""".format(schema=schema, table=table)

    try:
        cur.execute(create_table, {'schema': schema, 'table': table})
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise


def initialize_database(database,
                        user,
                        password,
                        host,
                        port,
                        force_drop=False,
                        hist_table='h_parkeervakken',
                        park_table='parkeervakken',
                        res_table='reserveringen',
                        date_table='datums',
                        schema='public'):
    """Setup the temporary and history tables in the database. If they already
    exist the creation of the table is skipped, unless :param:`force_drop` is
    true. In that case the tables are dropped if they exist and new tables are
    created. This also means that any data in a history table is lost.

    :type database: str
    :type schema: str
    :type hist_table: str
    :type park_table: str
    :type res_table: str
    :type date_table: str
    :type user: str
    :type password: str
    :type host: str
    :type port: int
    """

    c = re.compile('^[a-zA-Z0-9_-]+$')

    tables = [
        hist_table,
    ]

    for table in tables:
        if c.match(table) is None or c.match(schema) is None:
            # TODO
            print('Wrong table or schema name: {} {}'.format(table, schema))
            raise Exception

    conn = psycopg2.connect(database=database,
                            user=user,
                            password=password,
                            host=host,
                            port=port)

    cur = conn.cursor()

    create_hist_table(conn, cur, hist_table, schema, force_drop)
    create_parking_space_table(conn, cur, park_table, schema, force_drop)
    create_parking_reservation_table(conn, cur, res_table, schema, force_drop)
    create_date_table(conn, cur, date_table, schema, force_drop)


def main():
    args = setup_argparse()

    command = args.command

    database_credentials = {
        'database': args.database,
        'user': args.user,
        'password': args.password,
        'host': args.host,
        'port': args.port,
    }

    hist_table = args.history_table
    tmp_table = args.temporary_table
    park_table = args.park_table
    res_table = args.res_table
    date_table = args.date_table
    schema = args.schema

    if command == 'init':
        force_drop = args.force_drop
        initialize_database(hist_table=hist_table,
                            park_table=park_table,
                            res_table=res_table,
                            schema=schema,
                            force_drop=force_drop,
                            **database_credentials)
    elif command == 'update':
        source = pathlib.Path(args.source)
        target = args.target
        skip_import = args.skip_import
        skip_parking = args.skip_parking
        skip_reservations = args.skip_reservations
        skip_dates = args.skip_dates

        target = (
            pathlib.Path(target)
            if target is not None
            else target
        )

        import_data(hist_table=hist_table,
                    tmp_table=tmp_table,
                    park_table=park_table,
                    res_table=res_table,
                    date_table=date_table,
                    schema=schema,
                    source=source,
                    target=target,
                    skip_import=skip_import,
                    skip_dates=skip_dates,
                    skip_parking=skip_parking,
                    skip_reservations=skip_reservations,
                    **database_credentials)


if __name__ == '__main__':
    main()

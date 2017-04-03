#!/usr/bin/python3.5

import subprocess
import datetime
import argparse
import re
import shlex
import pathlib

import psycopg2
import logging


logging.basicConfig(level=logging.DEBUG)

log = logging.getLogger(__name__)

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
                        help=("The schema name where all the tables are "
                              "found. Default is public."))

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
    update_parser.add_argument('--interval',
                               dest='interval',
                               default='5 days')
    return parser.parse_args()


def find_latest_date(source, file_type):
    """
    :type source: pathlib.Path
    """

    last_date = None

    for path in source.glob('*.{}'.format(file_type)):
        match = stadsdeel_c.match(path.stem)
        _, date_string = match.groups()
        date = datetime.datetime.strptime(date_string, '%Y%m%d')

        if last_date is None or last_date < date:
            last_date = date

    return last_date


def import_data(database,
                user,
                password,
                host,
                port,
                source,
                skip_import=False,
                skip_dates=False,
                interval='1 year'):
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
    :type skip_import: bool
    :type skip_dates: bool
    """

    source = source.absolute()

    conn = psycopg2.connect(
        database=database,
        user=user,
        password=password,
        host=host,
        port=port)

    table_counts(conn)

    with conn:
        with conn.cursor() as cur:

            if not skip_import:
                latest_date = find_latest_date(source, 'shp')

                import_shape_data(source, latest_date, conn, cur)

            if not skip_dates:
                update_dates(conn, cur, interval)

    table_counts(conn)

    files = [
        'import_his_bm.sql',
        'import_bm_bv.sql',
    ]

    for filename in files:
        execute_sql(conn, filename)

    table_counts(conn)


def table_counts(conn):

    table_names = [
        'his.parkeervakken',
        'bm.parkeervakken',
        'bm.reserveringen_fiscaal',
        'bm.reserveringen_mulder',
        'bm.reserveringen_mulder_schoon',
        'bv.parkeervakken',
        'bv.reserveringen',
        'bv.reserveringen_fiscaal',
        'bv.reserveringen_mulder',
        'bv.reserveringen_mulder_schoon',
    ]

    log.debug("""

        his = source data'
        bm = Business Model (intermediate tables)
        bv = Business Views (final output tables)

    """)

    for table in table_names:
        with conn:
            with conn.cursor() as cursor:
                count_statement = f"SELECT COUNT(*) FROM {table};"
                count = 0
                try:
                    cursor.execute(count_statement)
                    results = cursor.fetchone()
                    count = results[0]
                except Exception as e:
                    pass
                    # conn.close()
                    # log.debug(e)

                log.info('Count %40s: %-10s', table, count)

    log.debug('\n\n')


def import_shape_data(source, latest_date, conn, cur):
    """Unzip all data in :param:`zip_file` and load the shape files.

    :type source: pathlib.Path
    :type latest_date: datetime.datetime
    :type conn: psycopg2.extensions.connection
    :type cur: psycopg2.extensions.connection
    """

    for shp_file in source.glob('*.shp'):
        match = stadsdeel_c.match(shp_file.stem)
        _, date_string = match.groups()
        file_date = datetime.datetime.strptime(date_string, '%Y%m%d')

        if latest_date != file_date:
            continue

        # print('Load', shp_file)
        log.debug('Load %s', shp_file)

        load_shape_file(conn, cur, shp_file)


def load_shape_file(conn, cur, shp_file):
    """Import data from shape files into the database. The shape files are
    imported by first creating an sql script using `shp2pgsql` and executing
    this on the database.

    :type conn: psycopg2.extensions.connection
    :type cur: psycopg2.extensions.connection
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

    if 'nietfiscaal' in shp_file.parts:
        stadsdeel += '_NF'

    stadsdeel = stadsdeel.lower().replace('-', '_')

    log.debug(stadsdeel)

    drop_table(conn, cur, 'parkeervakken', 'public')

    cmd_fmt = 'shp2pgsql -W LATIN1 -I {shape_file} public.parkeervakken'

    shp2pgsql_cmd = shlex.split(cmd_fmt.format(shape_file=str(shp_file)))

    # Create the sql statements for loading shape data into the database
    try:
        output = subprocess.check_output(shp2pgsql_cmd)
        shp_stmts = output.decode('UTF-8')
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
    update_history(conn, cur, stadsdeel, date)


def update_history(conn, cur, stadsdeel, date):
    """Copy data from :param:`tmp_table` to :param:`hist_table`. First all data
    with the same :param:`stadsdeel` and :param:`date` is deleted from
    :param:`hist_table`. After that new data is inserted.

    :type conn: psycopg2.extensions.connection
    :type cur: psycopg2.extensions.connection
    :type stadsdeel: str
    :type date: str
    """

    partition_table = create_partition(conn,
                                       cur,
                                       'parkeervakken',
                                       'his',
                                       stadsdeel)

    # First truncate partition
    truncate = ("TRUNCATE his.{part_table}".format(part_table=partition_table))

    try:
        cur.execute(truncate)
        conn.commit()
    except Exception:
        conn.close()
        raise

    # Insert data from the temporary table into the history table
    insert_stmt = """INSERT INTO his.{part_table}
    (
        parkeervak_id_md5,
        parkeer_id,
        buurtcode,
        straatnaam,
        soort,
        "type",
        aantal,
        kenteken,
        e_type,
        bord,
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
        eindtijd2,
        begintijd2,
        opmerking,
        tvm_begind,
        tvm_eindd,
        tvm_begint,
        tvm_eindt,
        tvm_opmerk,
        geom,
        stadsdeel,
        goedkeurings_datum
    )
    SELECT
        concat(parkeer_id,
                   '-',
                   tvm_begind,
                   '-',
                   tvm_begint),
        parkeer_id,
        buurtcode,
        straatnaam,
        soort,
        "type",
        aantal,
        kenteken,
        e_type,
        bord,
        CASE begintijd1 ~ '^[0-9][0-9]?[:;][0-9][0-9]?([:;][0-9][0-9]?)?$'
            WHEN TRUE THEN begintijd1
            ELSE NULL
        END,
        CASE eindtijd1 ~ '^[0-9][0-9]?[:;][0-9][0-9]?([:;][0-9][0-9]?)?$'
            WHEN TRUE THEN eindtijd1
            ELSE NULL
        END,
        ma_vr,
        ma_za,
        zo,
        ma,
        di,
        wo,
        "do",
        vr,
        za,
        CASE eindtijd2 ~ '^[0-9][0-9]?[:;][0-9][0-9]?([:;][0-9][0-9]?)?$'
            WHEN TRUE THEN eindtijd2
            ELSE NULL
        END,
        CASE begintijd2 ~ '^[0-9][0-9]?[:;][0-9][0-9]?([:;][0-9][0-9]?)?$'
            WHEN TRUE THEN begintijd2
            ELSE NULL
        END,
        opmerking,
        tvm_begind,
        tvm_eindd,
        CASE tvm_begint ~ '^[0-9][0-9]?[:;][0-9][0-9]?([:;][0-9][0-9]?)?$'
            WHEN TRUE THEN tvm_begint
            ELSE NULL
        END,
        CASE tvm_eindt ~ '^[0-9][0-9]?[:;][0-9][0-9]?([:;][0-9][0-9]?)?$'
            WHEN TRUE THEN tvm_eindt
            ELSE NULL
        END,
        tvm_opmerk,
        geom,
        %(stadsdeel)s,
        %(date)s
    FROM public.parkeervakken""".format(part_table=partition_table)

    try:
        cur.execute(insert_stmt, {'stadsdeel': stadsdeel, 'date': date})
        conn.commit()
    except Exception:
        conn.close()
        raise


def update_dates(conn, cur, interval='1 day'):
    """

    :type conn: psycopg2.extensions.connection
    :type cur: psycopg2.extensions.connection
    :type schema: str
    """

    truncate = "TRUNCATE bm.datums"

    try:
        cur.execute(truncate)
        conn.commit()
    except Exception:
        conn.close()
        raise

    insert = """INSERT INTO bm.datums
    SELECT datum, date_part('dow', datum)
    FROM (
        SELECT
            generate_series(min_date, max_date, interval '1d') as datum
        FROM (
            SELECT
                CURRENT_DATE AS min_date,
                CURRENT_DATE + interval %(interval)s AS max_date
            FROM his.parkeervakken
            GROUP BY goedkeurings_datum
        ) AS u
    ) as t"""

    try:
        cur.execute(insert, {'interval': interval})
        conn.commit()
    except Exception:
        conn.close()
        raise


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


def create_tables(database, user, password, host, port):
    """
    :type database: str
    :type user: str
    :type password: str
    :type host: str
    :type port: int
    """

    conn = psycopg2.connect(database=database,
                            user=user,
                            password=password,
                            host=host,
                            port=port)

    files = [
        'create_his_tables.sql',
        'create_bm_tables.sql',
        'create_bv_tables.sql',
        'create_views.sql',
    ]

    for filename in files:
        execute_sql(conn, filename)


def execute_sql(conn, filename):
    """
    :type conn: a Connection object
    :type filename: str
    """

    log.debug('SQL: %s', filename)

    with open(filename) as f:
        stmts = f.read()

        try:
            with conn.cursor() as c:
                c.execute(stmts)
            conn.commit()
        except Exception:
            conn.rollback()
            conn.close()
            raise


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

    if command == 'init':
        create_tables(**database_credentials)

    elif command == 'update':

        source = pathlib.Path(args.source)
        skip_import = args.skip_import
        skip_dates = args.skip_dates
        interval = args.interval

        import_data(source=source,
                    skip_import=skip_import,
                    skip_dates=skip_dates,
                    interval=interval,
                    **database_credentials)


if __name__ == '__main__':
    main()

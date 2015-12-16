import subprocess
import argparse
import re
import zipfile
import shutil
import shlex
import pathlib

import yaml

import psycopg2


def setup_argparse():
    parser = argparse.ArgumentParser()

    parser.add_argument('--config', type=pathlib.Path)

    subparsers = parser.add_subparsers()

    init_parser = subparsers.add_parser('initialize')
    init_parser.add_argument('--force-drop',
                             action='store_true',
                             default=False)
    init_parser.set_defaults(command='init')

    update_parser = subparsers.add_parser('update')
    update_parser.set_defaults(command='update')

    return parser.parse_args()


def clean_directory(directory):
    """
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
                hist_csv_table='h_csv_parkeervakken',
                tmp_csv_table='s_csv_parkeervakken',
                tmp_table='s_parkeervakken'):
    """
    :type database: str
    :type user: str
    :type password: str
    :type host: str
    :type port: int
    :type source: pathlib.Path
    :type target: pathlib.Path
    :type schema: str
    :type hist_table: str
    :type hist_csv_table: str
    :type tmp_csv_table: str
    :type tmp_table: str
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

    for f in source.iterdir():
        if f.suffix != '.zip':
            continue

        import_zip_data(f,
                        conn,
                        cur,
                        target,
                        schema,
                        hist_table,
                        hist_csv_table,
                        tmp_csv_table,
                        tmp_table)


def get_file(directory, suffix):
    """
    :type directory: pathlib.Path
    :type suffix: str
    """

    for f in directory.iterdir():
        if f.suffix == suffix:
            return f

    raise FileNotFoundError()


def import_zip_data(zip_file,
                    conn,
                    cur,
                    target,
                    schema,
                    hist_table,
                    hist_csv_table,
                    tmp_csv_table,
                    tmp_table):
    """
    :type conn: psycopg2.extensions.connection
    :type cur: psycopg2.extensions.connection
    :type zip_file: pathlib.Path
    :type target: patrhlib.Path
    """

    c = re.compile(r'^(?P<stadsdeel>[a-zA-Z]*)_parkeerhaven.*_'
                   r'(?P<date>[0-9]{8})$')

    clean_directory(target)

    with zipfile.ZipFile(str(zip_file)) as z:
        z.extractall(str(target))

    try:
        shp_file = get_file(target, '.shp')
    except FileNotFoundError:
        conn.close()
        raise

    try:
        csv_file = get_file(target, '.csv')
    except FileNotFoundError:
        conn.close()
        raise

    match = c.match(shp_file.stem)

    if match is None:
        conn.close()

        # TODO
        raise Exception

    stadsdeel, date = match.groups()

    if stadsdeel == '':
        stadsdeel = 'unknown'

    stadsdeel = stadsdeel.lower()

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

    # Truncate csv file
    truncate_csv = """TRUNCATE {schema}.{table}""".format(schema=schema,
                                                          table=tmp_csv_table)

    try:
        cur.execute(truncate_csv)
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise

    # Load csv file into temporary csv table in the database
    try:
        with csv_file.open(encoding='latin1') as f:
            copy_stmt = """COPY {schema}.{table} FROM STDIN
            WITH (
                FORMAT csv,
                HEADER TRUE,
                DELIMITER ';',
                QUOTE '"',
                ENCODING 'LATIN1',
                NULL ''
            )
            """.format(schema=schema, table=tmp_csv_table)
            cur.copy_expert(copy_stmt, f)
    except Exception:
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

    # Update date from the temporary csv table into the csv history table
    update_history(conn,
                   cur,
                   tmp_csv_table,
                   hist_csv_table,
                   schema,
                   stadsdeel,
                   date)


def update_history(conn,
                   cur,
                   tmp_table,
                   hist_table,
                   schema,
                   stadsdeel,
                   date,
                   columns='*'):
    """
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
    WHERE aanvraag_datum = %(date)s""".format(schema=schema,
                                              hist_table=partition_table)

    try:
        cur.execute(delete_stmt, {'date': date})
        conn.commit()
    except Exception:
        conn.close()
        raise

    # Insert data from the temporary table into the history table
    insert_stmt = """INSERT INTO {schema}.{hist_table}
    SELECT {columns}, %(stadsdeel)s AS stadsdeel, %(date)s AS aanvraag_datum
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


def create_partition(conn, cur, table, schema, stadsdeel):
    """
    :type conn: psycopg2.extensions.connection
    :type cur: psycopg2.extensions.connection
    :type table: str
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
    """
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
    """
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


def create_hist_table(conn, cur, table, schema, force_drop=False):
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
        ADD COLUMN "aanvraag_datum" varchar(40);
    ALTER TABLE "{schema}"."{table}"
        ADD PRIMARY KEY (parkeer_id, aanvraag_datum);
    """.format(schema=schema, table=table)

    try:
        cur.execute(create_table, {'schema': schema, 'table': table})
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise


def create_hist_csv_table(conn, cur, table, schema, force_drop=False):
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

    create_csv_table = """CREATE TABLE IF NOT EXISTS {schema}.{csv} (
        "parkeer_id" varchar(10),
        "buurtcode" varchar(20),
        "straatnaam" varchar(40),
        "x" varchar(10),
        "y" varchar(10),
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
        "tvm_opmerk" varchar(100),
        "stadsdeel" varchar(40),
        "aanvraag_datum" varchar(10)
    )""".format(schema=schema, csv=table)

    try:
        cur.execute(create_csv_table, {'schema': schema, 'table': table})
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise


def create_tmp_csv_table(conn, cur, table, schema, force_drop=False):
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

    create_csv_table = """CREATE TABLE IF NOT EXISTS {schema}.{csv} (
        "parkeer_id" varchar(10),
        "buurtcode" varchar(20),
        "straatnaam" varchar(40),
        "x" varchar(10),
        "y" varchar(10),
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
    )""".format(schema=schema, csv=table)

    try:
        cur.execute(create_csv_table, {'schema': schema, 'table': table})
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
                        tmp_csv_table='s_csv_parkeervakken',
                        hist_csv_table='h_csv_parkeervakken',
                        schema='public'):
    """
    :type database: str
    :type schema: str
    :type hist_table: str
    :type hist_table: str
    :type hist_csv_table: str
    :type csv_tmp_table: str
    :type user: str
    :type password: str
    :type host: str
    :type port: int
    """

    c = re.compile('^[a-zA-Z0-9_-]+$')

    tables = [
        hist_table,
        tmp_csv_table,
        hist_csv_table,
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
    create_tmp_csv_table(conn, cur, tmp_csv_table, schema, force_drop)
    create_hist_csv_table(conn, cur, hist_csv_table, schema, force_drop)


def main():
    args = setup_argparse()

    config_file = args.config
    command = args.command

    with config_file.open() as f:
        config = yaml.load(f.read())

    database_credentials = {
        'database': config['database'],
        'user': config['user'],
        'password': config['password'],
        'host': config['host'],
        'port': config['port'],
    }

    hist_table = config.get('history_table', 'h_parkeervakken')
    tmp_table = config.get('temporary_table', 's_parkeervakken')
    hist_csv_table = config.get('history_csv_table', 'h_csv_parkeervakken')
    tmp_csv_table = config.get('temporary_csv_table', 's_csv_parkeervakken')
    schema = config.get('schema', 'public')

    if command == 'init':
        force_drop = args.force_drop
        initialize_database(hist_table=hist_table,
                            tmp_csv_table=tmp_csv_table,
                            hist_csv_table=hist_csv_table,
                            schema=schema,
                            force_drop=force_drop,
                            **database_credentials)
    elif command == 'update':
        source = pathlib.Path(config['source'])
        target = config.get('target', None)

        target = (
            pathlib.Path(target)
            if target is not None
            else target
        )

        import_data(hist_table=hist_table,
                    tmp_table=tmp_table,
                    hist_csv_table=hist_csv_table,
                    tmp_csv_table=tmp_csv_table,
                    schema=schema,
                    source=source,
                    target=target,
                    **database_credentials)


if __name__ == '__main__':
    main()

import logging
import psycopg2
import psycopg2.errors
import psycopg2.extensions
from psycopg2.extensions import connection as Connection
from psycopg2.extras import RealDictCursor


log = logging.getLogger('app.db')

# define settings default values
PG_CONNECTION_STR = None

# override settings by local values
try:
    from conf.local_settings import *       # noqa: F401, F403
except ImportError:
    pass


def do_commit(conn: Connection):
    conn.commit()


def init_table(conn: Connection):
    log.warning('create table health_checks')

    sql = """
    CREATE TABLE public.health_checks (
        id uuid NOT NULL,
        check_name varchar NOT NULL,
        dt timestamp NULL,
        health bool NULL,
        status varchar NULL,
        duration numeric NULL,
        length int4 NULL, -- Content-Length
        sample varchar NULL
    );

    COMMENT ON COLUMN public.health_checks.length IS 'Content-Length';

    CREATE UNIQUE INDEX health_checks_id_idx ON public.health_checks (id);

    CREATE INDEX health_checks_name_idx ON public.health_checks (check_name,dt);
    """
    curr = conn.cursor()
    curr.execute(sql)
    do_commit(conn)


def check_table_exists(conn: Connection):
    log.debug('check if table health_checks exists')
    sql = """select id, check_name, dt, health, status, duration, length, sample from public.health_checks where 1=0"""
    curr = conn.cursor()
    curr.execute(sql)


def create_table_if_not_exists(conn: Connection):
    try:
        check_table_exists(conn)
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        init_table(conn)
    else:
        log.debug('table health_checks exists')


def get_connect():
    log.info('try connect to db')
    conn = psycopg2.connect(PG_CONNECTION_STR, cursor_factory=RealDictCursor)
    log.info('connected to db')
    create_table_if_not_exists(conn)
    return conn


def append_record(conn: Connection, rec: dict):
    log.debug(f'append record {rec["id"]} health:{rec["health"]} {rec["check_name"]}')
    sql = """insert into public.health_checks(id, check_name, dt, health, status, duration, length, sample)
    values (%(id)s, %(check_name)s, %(dt)s, %(health)s, %(status)s, %(duration)s, %(length)s, %(sample)s)
    ON CONFLICT DO NOTHING"""
    curr = conn.cursor()
    curr.execute(sql, rec)


DEC2FLOAT = psycopg2.extensions.new_type(
    psycopg2.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: float(value) if value is not None else None)
psycopg2.extensions.register_type(DEC2FLOAT)

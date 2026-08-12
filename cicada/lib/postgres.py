"""Backend PostgreSQL database library"""

# 2015-07-01 Louis Pieterse

from contextlib import contextmanager

import psycopg2

from cicada.lib import utils

# Create a PgSQL database connection based on definition requested


def db_cicada(dbname=None):
    """Connect to PostgreSQL backend database"""
    definitions = utils.load_config()

    host = definitions["db_cicada"]["host"]
    port = definitions["db_cicada"]["port"]
    if not dbname:
        dbname = definitions["db_cicada"]["dbname"]
    user = definitions["db_cicada"]["user"]
    password = definitions["db_cicada"]["password"]

    conn = psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user, password=password, sslmode="require", application_name="cicada"
    )
    conn.autocommit = True

    return conn


@contextmanager
def db_cicada_cursor(dbname=None):
    """Yield a Cicada connection and cursor, closing both on every exit path."""
    conn = db_cicada(dbname)
    cursor = None
    try:
        cursor = conn.cursor()
        yield conn, cursor
    finally:
        try:
            if cursor is not None:
                cursor.close()
        finally:
            conn.close()


def escape_upsert_string(regular_string):
    """Makes string INSERT safe"""
    return regular_string.replace("'", "''")

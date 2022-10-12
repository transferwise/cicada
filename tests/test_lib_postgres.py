"""test_lib_postgres.py"""

from cicada.lib import postgres


def test_cicada_db_connection():
    """test_cicada_db_connection"""
    db_conn = postgres.db_cicada()
    assert db_conn.autocommit


def test_cicada_db_connection_close():
    """test_cicada_db_connection_close"""
    db_conn = postgres.db_cicada()
    db_conn.close()
    assert db_conn.closed


def test_return_data():
    """test_return_data"""
    db_conn = postgres.db_cicada()
    db_cur = db_conn.cursor()
    sqlquery = "SELECT 1"

    db_cur.execute(sqlquery)
    row = db_cur.fetchone()
    db_cur.close()
    db_conn.close()

    assert str(row[0]) == str(1)


def test_escape_upsert_string():
    """test_escape_upsert_string"""

    some_text = "'some_text'"
    some_text = postgres.escape_upsert_string(some_text)

    assert some_text == str("''some_text''")

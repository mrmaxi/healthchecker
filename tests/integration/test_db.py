from pytest import mark, param, fixture

import db
from .conftest import clear_db
from ..unit.test_db_writer import TEST_RECORD1, TEST_RECORD2


@fixture(autouse=True, scope='module')
def my_setup_module(temp_conn):
    clear_db(temp_conn)


def get_record(conn: db.Connection, id: str):
    curr = conn.cursor()
    sql = 'select * from health_checks where id=%(id)s'
    curr.execute(sql, dict(id=id))
    rec = curr.fetchone()
    return dict(rec) if rec else None


@mark.parametrize("record", [
    param(TEST_RECORD1, id='record1'),
    param(TEST_RECORD2, id='record2'),
])
def test_append_record(record: dict, temp_conn):
    assert not get_record(temp_conn, record['id']), "Isn't clear database, record already exists"
    db.append_record(temp_conn, record)
    actual = get_record(temp_conn, record['id'])
    db.do_commit(temp_conn)
    assert actual == record

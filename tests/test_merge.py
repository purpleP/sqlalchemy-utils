from itertools import chain

import pytest

from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects import sqlite, mysql, postgresql
from sqlalchemy_utils.compilers import Merge

Base = declarative_base()


table = Table(
    'foos',
    MetaData(),
    Column('id', Integer, primary_key=True),
    Column('a', String(10)),
    Column('b', String(10)),
)


sqlite_stmt = (
    'INSERT OR REPLACE INTO foos (id, a, b) VALUES '
    '(?, ?, (SELECT foos.b \nFROM foos \nWHERE foos.id = ?)), '
    '(?, ?, (SELECT foos.b \nFROM foos \nWHERE foos.id = ?))'
)
mysql_stmt = (
    'INSERT INTO foos (id, a) VALUES '
    '(%s, %s), (%s, %s) '
    'ON DUPLICATE KEY UPDATE a = VALUES(a)'
)
postgres_stmt = (
    'INSERT INTO foos (id, a) VALUES '
    '(%(id_m0)s, %(a_m0)s), (%(id_m1)s, %(a_m1)s) '
    'ON CONFLICT (id) DO UPDATE '
    'SET a = excluded.a'
)


@pytest.mark.parametrize(
    'dialect,expected_stmt',
    ((sqlite, sqlite_stmt), (mysql, mysql_stmt), (postgresql, postgres_stmt))
)
def test_merge(dialect, expected_stmt):
    values = (
        dict(id=1, a='a'),
        dict(id=2, a='b'),
    )
    compiled_stmt = Merge(table, values).compile(dialect=dialect.dialect())
    assert expected_stmt == str(compiled_stmt)

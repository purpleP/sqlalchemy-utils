from itertools import chain

import pytest

import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects import sqlite, mysql, postgresql
from sqlalchemy_utils.compilers import Merge, visit_in

Base = declarative_base()


table = sa.Table(
    'foos',
    sa.MetaData(),
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('a', sa.String(10)),
    sa.Column('b', sa.String(10)),
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


def test_sqlite_in_with_multiple_columns():
    vals = (
        (1, '1'),
        (2, '2'),
    )
    cols = table.c.id, table.c.a
    select_in = sa.select(cols).where(sa.tuple_(*cols).in_(vals))
    select_and = sa.select(cols).where(sa.or_(
        sa.and_(col == value for col, value in zip(cols, vs))
        for vs in vals
    ))
    compiled_in = select_in.compile(dialect=sqlite.dialect())
    compiled_and = select_and.compile(dialect=sqlite.dialect())
    assert str(compiled_in) == str(compiled_and)

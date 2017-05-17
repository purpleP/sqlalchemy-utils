from itertools import chain

import pytest

from sqlalchemy import (
    Column,
    String,
    Integer,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects import sqlite, mysql
from sqlalchemy_utils.compilers import Merge

Base = declarative_base()


class Foo(Base):
    __tablename__ = 'foos'
    id = Column(Integer, primary_key=True)
    a = Column(String(10))
    b = Column(String(10))


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


@pytest.mark.parametrize(
    'dialect,expected_stmt',
    ((sqlite, sqlite_stmt), (mysql, mysql_stmt))
)
def test_merge(dialect, expected_stmt):
    values = (
        dict(id=1, a='a'),
        dict(id=2, a='b'),
    )
    compiled_stmt = Merge(Foo, values).compile(dialect=dialect.dialect())
    assert expected_stmt == str(compiled_stmt)

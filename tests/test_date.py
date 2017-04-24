import pytest
from sqlalchemy import (
    create_engine,
    Column,
    Date,
    String,
    Integer,
    ForeignKey
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import select
from sqlalchemy.sql.expression import union, literal, alias
from itertools import chain
from subprocess import call
from datetime import date
from sqlalchemy.dialects.mysql import TINYINT
from tests.fixtures import Base, session, sqlite, mysql
from sqlalchemy_utils.compilers import MakeADate


dates = (
     date(2016, 1, 1),
     date(2016, 1, 2),
)


class Foo(Base):
    __tablename__ = 'foo'
    id = Column(Integer, primary_key=True)
    foo = Column(TINYINT(2))


def test_date(session):
    dates = (
        date(2016, 1, 1),
        date(2016, 1, 2),
    )
    selects = tuple(select((MakeADate(d),)) for d in dates)
    data = alias(union(*selects, use_labels=True), 'dates')
    stmt = select((data,))
    result = session.execute(stmt).fetchall()
    assert tuple(chain.from_iterable(result)) == dates



def test_mysql_to_sqlite(session):
    foo = Foo(foo=1)
    session.add(foo)
    session.commit()
    assert [(1,)] == session.query(Foo.foo).all()

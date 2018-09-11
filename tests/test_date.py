from datetime import date
from itertools import chain

import sqlalchemy as sa

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.expression import union, alias
from sqlalchemy.dialects.mysql import TINYINT

from sqlalchemy_utils.compilers import MakeADate
from tests.fixtures import Base, session, sqlite, mysql


dates = (
    date(2016, 1, 1),
    date(2016, 1, 2),
)


class Foo(Base):
    __tablename__ = 'foo'
    id = sa.Column(sa.Integer, primary_key=True)
    foo = sa.Column(TINYINT(2))


def test_date(session):
    dates = (
        date(2016, 1, 1),
        date(2016, 1, 2),
    )

    selects = tuple(sa.select((MakeADate(d),)) for d in dates)
    data = alias(union(*selects, use_labels=True), 'dates')
    stmt = sa.select((data,))
    result = session.execute(stmt).fetchall()
    assert tuple(chain.from_iterable(result)) == dates



def test_mysql_to_sqlite(session):
    foo = Foo(foo=1)
    session.add(foo)
    session.commit()
    assert [(1,)] == session.query(Foo.foo).all()

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
from sqlalchemy.ext.compiler import compiles
from fixtures import Base, session, sqlite, mysql
from compilers import MakeADate


dates = (
     date(2016, 1, 1),
     date(2016, 1, 2),
)

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

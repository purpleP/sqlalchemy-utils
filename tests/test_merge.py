from itertools import chain

import pytest

from sqlalchemy import (
    create_engine,
    Column,
    Date,
    String,
    Integer,
    ForeignKey
)
from sqlalchemy.sql.expression import insert
from sqlalchemy.orm import relationship
from sqlalchemy_utils.compilers import Merge
from tests.fixtures import Base, session, sqlite, mysql


class Foo(Base):
    __tablename__ = 'foos'
    id = Column(Integer, primary_key=True)
    a = Column(String(10))
    change = Column(String(10))
    b = Column(String(10))

    def __repr__(self):
        return 'Foo a={a}, b={b}'.format(a=self.a, b=self.b)

    def to_dict(self):
        return {'a': self.a, 'b': self.b}

    @staticmethod
    def from_dict(dict_):
        return Foo(**dict_)


@pytest.mark.parametrize('initial_items,items_to_merge', (
    (
        {},
        {
            1: {'id': 1, 'change': '1', 'a': '1', 'b': '1'}
        },
    ),
    (
        {1: {'id': 1, 'change': '1', 'a': '1', 'b': '1'}},
        {
            1: {'id': 1, 'change': '1', 'a': '2', 'b': '3'},
            2: {'id': 2, 'change': '1', 'a': '2', 'b': '2'},
        },
    ),
))
def test_merge(session, initial_items, items_to_merge):
    items = {
        1: {'id': 1, 'change': '1', 'a': 'a', 'b': 'b'},
    }
    session.execute(insert(Foo, tuple(initial_items.values())))
    session.execute(Merge(Foo, tuple(items.values())))
    session.commit()
    foos = {
        foo.id: foo._asdict()
        for foo in session.query(Foo.id, Foo.change, Foo.a, Foo.b).all()
    }
    assert dict(chain(initial_items.items(), items.items())) == foos

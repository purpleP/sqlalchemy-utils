from inspect import getmro, getmembers, isclass
from itertools import tee
from functools import reduce, partial
from operator import eq

import re

from sqlalchemy import Date, select
from sqlalchemy.dialects import mysql, postgresql
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.elements import Cast
from sqlalchemy.sql.expression import Insert, insert
from sqlalchemy import and_, UniqueConstraint, PrimaryKeyConstraint


mysql_types = tuple(m for name, m in getmembers(mysql, isclass) if name.isupper())


class MakeADate(Cast):
    def __init__(self, elem):
        super(MakeADate, self).__init__(elem, Date)


@compiles(MakeADate)
def _default_date(elem, compiler, **kw):
    return compiler.visit_cast(elem, **kw)


def compiles_many(func=None, to=None, types=()):
    if not func:
        return partial(compiles_many, to=to, types=types)
    return reduce(lambda f, t: compiles(t, to)(f), types, func)


@compiles_many(to='sqlite', types=mysql_types)
def mysql_base(type_, compiler, **kwargs):
    mro = getmro(type_.__class__)
    basetype = next(n for p, n in pairwise(mro) if p.__module__ != n.__module__)
    some = getattr(compiler, 'visit_' + basetype.__name__.upper())(basetype, **kwargs)
    return some


def pairwise(sequence):
    a, b = tee(sequence)
    next(b)
    return zip(a, b)


@compiles(MakeADate, 'sqlite')
def _sqlite_date(elem, compiler, **kw):
    return compiler.process(elem.clause, **kw)


class Merge(Insert):
    def __init__(self, table, values):
        super(Merge, self).__init__(table, values)
        self.table = table
        self.values = values


@compiles(Merge, 'postgresql')
def postgres_merge(merge_stmt, compiler, **kwargs):
    table = merge_stmt.table
    primary_key = table.primary_key
    stmt = postgresql.insert(table, merge_stmt.values)
    stmt = stmt.on_conflict_do_update(
        index_elements=['id'],
        set_={
            k: getattr(stmt.excluded, k)
            for k in stmt.parameters[0] if k not in primary_key
        }
    )
    return compiler.process(stmt, **kwargs)


@compiles(Merge, 'mysql')
def mysql_merge(merge_stmt, compiler, **kwargs):
    stmt = mysql.insert(merge_stmt.table, merge_stmt.values)
    update = {
        name: getattr(stmt.inserted, name)
        for name in stmt.parameters[0] if name not in stmt.table.primary_key
    }
    stmt = stmt.on_duplicate_key_update(**update)
    return compiler.process(stmt, **kwargs)


@compiles(Merge, 'sqlite')
def sqlite_merge(merge_stmt, compiler, **kwargs):
    dummy = insert(merge_stmt.table, merge_stmt.values)
    values, table = dummy.parameters, dummy.table
    primary_key = table.primary_key
    unique_columns = next(
        c.columns
        for c in table.constraints
        if isinstance(c, (PrimaryKeyConstraint, UniqueConstraint))
        and all(c.name in values[0].keys() for c in c.columns)
    )
    other_columns = tuple(c for c in table.c if c.name not in values[0])
    def make_select(column, value):
        return select((column,)).where(
            and_(uc == value[uc.name] for uc in unique_columns)
        )
    values = tuple(
        {**v, **{c.name: make_select(c, v) for c in other_columns}}
        for v in values
    )
    stmt = insert(table, values).prefix_with('OR REPLACE')
    return compiler.process(stmt, **kwargs)

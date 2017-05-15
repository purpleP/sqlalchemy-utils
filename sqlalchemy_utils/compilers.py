from inspect import getmro, getmembers, isclass
from itertools import tee
from functools import reduce, partial

import re

from sqlalchemy.sql.expression import Executable, ClauseElement
from sqlalchemy import SMALLINT, Date, Integer
from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.elements import Cast
from sqlalchemy.sql.expression import Insert, insert


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
    def __init__(self, table, values, keys=None):
        super(Merge, self).__init__(table, values)
        self.keys = keys
        self.table = table
        self.values = values


def has_autoincrement(pk):
    return (
        len(pk.columns) == 1
        and isinstance(pk.columns.values()[0].type, Integer)
        and pk.columns.values()[0].autoincrement
    )


def used_columns(stmt):
    columns = stmt.parameters
    if stmt._has_multi_parameters:
        columns = columns[0]
    return columns.keys()


@compiles(Merge, 'postgresql')
def postgres_merge(merge_stmt, compiler, **kwargs):
    stmt = postgresql.insert(merge_stmt.table, merge_stmt.values)
    column_names = next(iter(stmt.parameters)).keys()
    stmt = stmt.on_conflict_do_update(
        index_elements=merge_stmt.keys or stmt.table.primary_key,
        set_={name: getattr(stmt.excluded, name) for name in column_names},
    )
    return compiler.visit_insert(stmt)


@compiles(Merge, 'mysql')
def mysql_merge(stmt, compiler, **kwargs):
    columns = used_columns(stmt)
    pk = stmt.table.primary_key
    autoinc = pk.columns.values()[0] if has_autoincrement(pk) else None
    if autoinc in columns:
        columns.remove(pk)
    insert = compiler.visit_insert(insert_stmt, **kwargs)
    ondup = 'ON DUPLICATE KEY UPDATE'
    updates = ', '.join(
        '{name} = VALUES({name})'.format(name=compiler.preparer.quote(c.name))
        for c in insert_stmt.table.columns
        if c.name in columns
    )
    if autoinc is not None:
        last_id = '{inc} = LAST_INSERT_ID({inc})'.format(inc=autoinc)
        if updates:
            updates = ', '.join((last_id, updates))
        else:
            updates = last_id
    upsert = ' '.join((insert, ondup, updates))
    return upsert


@compiles(Merge, 'sqlite')
def sqlite_merge(insert_stmt, compiler, **kwargs):
    insert = compiler.visit_insert(insert_stmt, **kwargs)
    return re.sub('(INSERT)', r'\1 OR REPLACE', insert)

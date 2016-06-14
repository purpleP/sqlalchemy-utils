import re
from sqlalchemy import Integer, Date
from sqlalchemy.sql.expression import Insert, insert
from sqlalchemy.sql.elements import Cast
from sqlalchemy.ext.compiler import compiles


class MakeADate(Cast):
     def __init__(self, elem):
         super(MakeADate, self).__init__(elem, Date)


@compiles(MakeADate)
def _default_date(elem, compiler, **kw):
     return compiler.visit_cast(elem, **kw)


@compiles(MakeADate, "sqlite")
def _sqlite_date(elem, compiler, **kw):
     return compiler.process(elem.clause, **kw)


class Merge(Insert):
    pass


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


def format_column(column_name):
    if column_name in ('time', 'change'):
        return '`{}`'.format(column_name)
    else:
        return column_name


@compiles(Merge, 'mysql')
def mysql_merge(insert_stmt, compiler, **kwargs):
    columns = used_columns(insert_stmt)
    pk = insert_stmt.table.primary_key
    autoinc = pk.columns.values()[0] if has_autoincrement(pk) else None
    if autoinc in columns:
        columns.remove(pk)
    insert = compiler.visit_insert(insert_stmt, **kwargs)
    ondup = 'ON DUPLICATE KEY UPDATE'
    updates = ', '.join(
        '{name} = VALUES({name})'.format(name=format_column(c.name))
        for c in insert_stmt.table.columns
        if c.name in columns
    )
    if autoinc is not None:
        last_id = '%s = LAST_INSERT_ID(%s)' % (autoinc, autoinc)
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

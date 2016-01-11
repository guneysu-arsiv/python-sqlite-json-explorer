#!/usr/bin/env python
# coding: utf-8
__author__ = u'Ahmed Şeref GÜNEYSU'

import bottle
from bottle import route, get
import sqlite3

sqlite3.enable_callback_tracebacks(True)

db = sqlite3.connect("../db.sqlite3")
db.row_factory = sqlite3.Row
db.isolation_level = None

cursor = db.cursor()


def get_tables():
    tables_dict = dict()

    # cursor.execute("SELECT * FROM sqlite_master WHERE type != 'index';")
    cursor.execute("SELECT * FROM sqlite_master WHERE type = 'table';")
    # cursor.execute("SELECT * FROM sqlite_master WHERE type != 'index';")
    data = dict()
    for row in [c for c in cursor if c['name'] != 'sqlite_sequence']:
        key = row['name'].replace(' ', '_')
        tables_dict[key] = dict(
                name=row['name'],
                # ddl=row['sql'],
                type=row['type'],
                schema='http://localhost:8080/schema/%s' % key,
                data='http://localhost:8080/data/%s' % key)

    # data = [_[0] for _ in cursor.fetchall() if _[0] != 'sqlite_sequence']
    return tables_dict


@route('/')
def tables():
    return get_tables()


def table_columns(table):
    table = get_tables()[table]['name']
    cursor.execute("PRAGMA table_info('{0}');".format(table))
    data = [c['name'] for c in cursor]
    return data


@route('/schema/<_table>')
def table_info(_table):
    table = get_tables()[_table]['name']

    cursor.execute("PRAGMA table_info('{0}');".format(table))
    schema_info = [dict(
            name=_['name'],
            type=_['type'],
            is_key=_['pk'] == 1,
            is_required=_['notnull'] == 1,
            default_value=_['dflt_value']) for _ in cursor]

    return dict(info=schema_info)


@route('/data/<_table>/<_id>')
def getbyid(_table, _id):
    table = get_tables()[_table]['name']

    columns = table_columns(_table)
    db.row_factory = sqlite3.Row

    for record in cursor.execute(
            'SELECT * FROM "{0}" WHERE {1} = ?;'.format(table, columns[0]),
            (_id,)):
        tmp = {k: record[i] for i, k in enumerate(columns)}

    return tmp


@route('/data/<_table>')
def table_data(_table):
    table = get_tables()[_table]['name']
    columns = table_columns(_table)
    db.row_factory = sqlite3.Row

    data = list()
    for record in cursor.execute('SELECT * FROM "{0}";'.format(table)):
        tmp = {k: record[i] for i, k in enumerate(columns)}
        tmp['Uri'] = 'http://localhost:8080/data/{0}/{1}'.format(_table,
                                                                 record[0])
        data.append(tmp)
    return dict(data=data)


def employees():
    import marshal
    cursor.execute('SELECT * FROM Employees LIMIT 1 OFFSET 1;')
    for r in cursor:
        a = marshal.loads(r[14])
        print(a)


if __name__ == '__main__':
    get_tables()

    bottle.run(host='localhost', port=8080, debug=True, reloader=True)
    # employees()

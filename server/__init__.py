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


@route('/')
def tables():
    # cursor.execute("SELECT * FROM sqlite_master WHERE type != 'index';")
    cursor.execute("SELECT * FROM sqlite_master WHERE type = 'table';")
    # cursor.execute("SELECT * FROM sqlite_master WHERE type != 'index';")
    data = dict()
    for row in [c for c in cursor if c['name'] != 'sqlite_sequence']:
        data[row['name']] = dict(
                name=row['name'],
                # ddl=row['sql'],
                type=row['type'],
                schema='http://localhost:8080/schema/' + row['name'],
                data='http://localhost:8080/data/' + row['name'])

    # data = [_[0] for _ in cursor.fetchall() if _[0] != 'sqlite_sequence']
    return data


def table_columns(table):
    cursor.execute("PRAGMA table_info('{0}');".format(table))
    data = [c['name'] for c in cursor]
    return data


@route('/schema/<table>')
def table_info(table):
    cursor.execute("PRAGMA table_info('{0}');".format(table))
    schema_info = [dict(
            name=_['name'],
            type=_['type'],
            is_key=_['pk'] == 1,
            is_required=_['notnull'] == 1,
            default_value=_['dflt_value']) for _ in cursor]

    return dict(info=schema_info)


@route('/data/<table>/<_id>')
def getbyid(table, _id):
    columns = table_columns(table)
    db.row_factory = sqlite3.Row

    for record in cursor.execute(
            'SELECT * FROM "{0}" WHERE {1} = ?;'.format(table, columns[0]),
            (_id,)):
        tmp = {k: record[i] for i, k in enumerate(columns)}

    return tmp


@route('/data/<table>')
def table_data(table):
    columns = table_columns(table)
    db.row_factory = sqlite3.Row

    data = list()
    for record in cursor.execute('SELECT * FROM "{0}";'.format(table)):
        tmp = {k: record[i] for i, k in enumerate(columns)}
        tmp['Uri'] = 'http://localhost:8080/data/{0}/{1}'.format(table,
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
    # bottle.run(host='localhost', port=8080, debug=True, reloader=True)
    employees()

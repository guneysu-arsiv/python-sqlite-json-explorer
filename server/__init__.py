#!/usr/bin/env python
# coding: utf-8
__author__ = u'Ahmed Şeref GÜNEYSU'

import sqlite3

import bottle
from bottle import Bottle, response
from bottle import route

sqlite3.enable_callback_tracebacks(True)

db = sqlite3.connect("../db.sqlite3",detect_types=sqlite3.PARSE_DECLTYPES)
db.row_factory = sqlite3.Row
db.isolation_level = None

cursor = db.cursor()

app = Bottle()


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
                foreign_keys='http://localhost:8080/fk/%s' % key,
                data='http://localhost:8080/data/%s' % key)

    # data = [_[0] for _ in cursor.fetchall() if _[0] != 'sqlite_sequence']
    return tables_dict


def table_columns(_table):
    table = get_tables()[_table]['name']
    cursor.execute("PRAGMA table_info('{0}');".format(table))
    data = [(c['name'], c['type']) for c in cursor]
    return data


@app.route('/')
def tables():
    return get_tables()


def get_fk(_table):
    table = get_tables()[_table]['name']
    keys = ["seq", "table", "from", "to", "on_update", "on_delete", "match"]

    sql = "PRAGMA FOREIGN_KEY_LIST('{0}');".format(table)
    cursor.execute(sql)
    data = []
    for c in cursor:
        data.append({k: c[k] for k in keys})

    return dict(data=data)


@app.route('/fk/<_table>')
def fk(_table):
    return get_fk(_table)


@app.route('/schema/<_table>')
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


def getby_id(_table, _id):
    table = get_tables()[_table]['name']

    columns = table_columns(_table)
    db.row_factory = sqlite3.Row

    data = dict()

    for record in cursor.execute(
            'SELECT * FROM "{0}" WHERE {1} = ?;'.format(table, columns[0][0]),
            (_id,)):
        data = dict()
        col_types = [c[0] for c in columns if not c[1] == u'BLOB']
        for i, k in enumerate(col_types):
            # data[k] = record[str(k)]
            data[k] = record[str(k)]

            # continue
            # data = {k: record[i] for i, k in
            #         enumerate([c[0] for c in columns if not c[1] == u'BLOB'])}
    return data


@app.route('/data/<_table>/<_id>')
def getbyid(_table, _id):
    data = getby_id(_table, _id)

    foreign_key = get_fk(_table)
    for fk in foreign_key['data'][:]:
        subdoc = getby_id(fk['table'].replace('_', ' '), data[fk['to']])
        data[fk['from']] = subdoc

    return data


@app.route('/foo')
def foo():
    data = []
    for e in cursor.execute('SELECT * FROM Categories LIMIT 10;'):
        data.append([repr(e[3]), unicode(e[3])])
    return dict(data=data)



@app.route('/data/<_table>')
def table_data(_table):
    table = get_tables()[_table]['name']
    columns = table_columns(_table)
    db.row_factory = sqlite3.Row
    col_types = [c[0] for c in columns if not c[1] == u'BLOB']

    data = list()
    for record in cursor.execute('SELECT * FROM "{0}" LIMIT 10;'.format(table)):
        tmp = dict()
        for i, k in enumerate(col_types):
            tmp[k] = record[str(k)]
        # tmp = {k: record[i] for i, k in enumerate(columns)}
        tmp['Uri'] = 'http://localhost:8080/data/{0}/{1}'.format(_table,
                                                                 record[0])
        data.append(tmp)
    return dict(data=data)


def employees():
    """
    :return:
     FIXME Can not handle binary data
    """
    raise NotImplemented()
    import marshal
    cursor.execute('SELECT * FROM Employees LIMIT 1 OFFSET 1;')
    for r in cursor:
        a = marshal.loads(r[14])
        print(a)


@app.hook('after_request')
def enable_cors():
    """
    You need to add some headers to each request.
    Don't use the wildcard '*' for Access-Control-Allow-Origin in production.
    """
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers[
        'Access-Control-Allow-Methods'] = 'PUT, GET, POST, DELETE, OPTIONS'
    response.headers[
        'Access-Control-Allow-Headers'] = 'Origin, Accept, Content-Type, X-Requested-With, X-CSRF-Token'


if __name__ == '__main__':
    # get_tables()

    bottle.run(app, host='localhost', port=8080, debug=True, reloader=True)
    # employees()

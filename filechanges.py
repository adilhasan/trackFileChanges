import os
import sys
import sqlite3
import optparse

def run(db_file):
    '''Run the program'''
    table = "mytable"
    conn = create_db(db_file)

    if table_exists(table, conn):
        print("table exists %s" % table)
    else:
        print("table %s does not exist" % table)


def create_db(db_file):
    '''Create the database if it doesn't exist'''
    try:
        conn = sqlite3.connect(db_file, timeout=2)
    except BaseException as err:
        print(str(err))
        conn = None
    return conn


def cursor(conn, query, args):
    '''Create a cursor and run the query'''
    result = False
    cur = conn.cursor()

    try:
        cur.execute(query, args)
        rows = cur.fetchall()
        if len(rows) > 0:
            result = True
    except sqlite3.OperationalError as err:
        print(str(err))
        if cur != None:
            cur.close()
    return result


def table_exists(table, conn):
    '''Check if a table exists'''
    result = False
    query = "select name from sqlite_master where type = 'table' and name = ?"
    args = (table,)
    result = cursor(conn, query, args)
    return result
if __name__ == "__main__":
    parser = optparse.OptionParser()
    parser.add_option("-d", "--database", dest="db_file", default="./files.db")
    (options, args) = parser.parse_args()

    run(options.db_file)
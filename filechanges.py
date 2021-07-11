import os
import sys
import sqlite3
import optparse
import hashlib


def run(db_file, folder):
    '''Run the program'''
    # For now just a dummy
    setup_tracking_table("afile.txt", "1234", db_file)


def setup_tracking_table(fname, md5, db_file):
    '''Setup the files table'''
    create_tracking_table(db_file)
    create_tracking_table_idx(db_file)
    insert_tracking_table(fname, md5, db_file)


def create_db(db_file):
    '''Create the database if it doesn't exist'''
    try:
        conn = sqlite3.connect(db_file, timeout=2)
    except BaseException as err:
        print(str(err))
        conn = None
    return conn


def md5_in_db(db_file, fname):
    items = []
    query = "select md5 from files where fname = ?"
    try:
        conn = create_db(db_file)
        if conn:
            if table_exists("files", conn):
                try:
                    cur = conn.cursor()
                    cur.execute(query, (fname,))
                    for row in cur:
                        items.append(row[0])
                except sqlite3.OperationalError as err:
                    if cur:
                        cur.close()
                finally:
                    if cur:
                        cur.close()
    except sqlite3.OperationalError as err:
        if conn:
            conn.close()
    finally:
        if conn:
            conn.close()
    return items


def create_tracking_table(db_file):
    '''Create the table'''
    result = False
    query = "create table files(file text, md5 text)"
    try:
        conn = create_db(db_file)
        if conn is not None:
            if not table_exists("files", conn):
                try:
                    cur = conn.cursor()
                    cur.execute(query)
                except sqlite3.OperationalError as err:
                    print(str(err))
                    if cur:
                        cur.close()
                finally:
                    conn.commit()
                    if cur:
                        cur.close()
                    result = True
    except sqlite3.OperationalError as err:
        print(str(err))
        if conn:
            conn.close()
    finally:
        if conn:
            conn.close()
    return result


def create_tracking_table_idx(db_file):
    '''Create an indec for the table'''
    table = "files"
    query = "create index idx on files (file, md5)"
    run_query(db_file, query, None)


def run_query(db_file, query, args):
    '''Run the query'''
    conn = create_db(db_file)

    try:
        if conn is not None:
            if table_exists("files", conn):
                cur = conn.cursor()
                try:
                    if args:
                        cur.execute(query, args)
                    else:
                        cur.execute(query)
                except sqlite3.OperationalError as err:
                    print(str(err))
                    if cur:
                        cur.close()
                finally:
                    conn.commit()
                    if cur:
                        cur.close()
                    result = True
    except sqlite3.OperationalError as err:
        print(str(err))
        if conn:
            conn.close()
    finally:
        if conn:
            conn.close()


def update_tracking_table(fname, md5, db_file):
    '''Update the files table'''

    query = "update files set md5=? where file = ?"
    args = (md5, fname)
    run_query(db_file, query, args)


def insert_tracking_table(fname, md5, db_file):
    '''Insert into the files table'''
    query = "insert into files(file, md5) values(?,?)"
    args = (fname, md5)
    run_query(db_file, query, args)


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


def has_changed(fname, md5, db_file):
    '''Check if a file has changed'''
    md5s = md5_in_db(db_file, fname)
    if len(md5s) == 1:
        if md5s[0] != md5:
            update_tracking_table(fname, md5, db_file)
        else:
            setup_tracking_table(fname, md5, db_file)


def get_fileext(fname):
    '''Get the filename extension'''
    ext = os.path.splitext(fname)[1]
    return ext


def get_mod_date(fname):
    '''Get the file modified date'''
    try:
        mtime = os.path.getmtime(fname)
    except:
        mtime = 0
    return mtime


def md5_short(fname):
    '''Get the md5 file hash'''
    md5_hash = hashlib.md5()
    with open(fname, 'r') as fin:
        while True:
            buff = fin.buffer(8192)
            if not buff:
                break
            md5_hash.update(buff)
        md5_digest = md5_hash.hexdigest()
    md5 = str(md5_digest).lower()
    return md5


if __name__ == "__main__":
    parser = optparse.OptionParser()
    parser.add_option("-d", "--database", dest="db_file", default="./files.db")
    parser.add_option("-f", "--folder", dest="folder")
    (options, args) = parser.parse_args()

    run(options.db_file, options.folder)
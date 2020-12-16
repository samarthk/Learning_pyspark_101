import sqlite3
from sqlite3 import Error

#Check DB ORACLE/SQLLite JDBC


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print("Connected!!")
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        if conn:
            print("Connected!! - can close now")

#           conn.close()

def select_all_tasks(conn):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM Album where AlbumId < 500")

    rows = cur.fetchall()

    for row in rows:
        print(row)


if __name__ == '__main__':
    create_connection(r"/home/sammy/snap/dbeaver-ce/90/.local/share/DBeaverData/workspace6/.metadata/sample-database-sqlite-1/Chinook.db")
#
conn = sqlite3.connect(r"/home/sammy/snap/dbeaver-ce/90/.local/share/DBeaverData/workspace6/.metadata/sample-database-sqlite-1/Chinook.db")

select_all_tasks(conn)



# jdbc:sqlite:/home/sammy/snap/dbeaver-ce/90/.local/share/DBeaverData/workspace6/.metadata/sample-database-sqlite-1/Chinook.db




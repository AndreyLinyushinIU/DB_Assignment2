# import the sql and connect libraries for psycopg2
from psycopg2 import sql, connect


# define a function that gets the column names from a PostgreSQL table
def get_columns(connection, table_name):
    cursor = connection.cursor()
    #col_names_query = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{}';".format(table_name)
    col_names_query = """SELECT attrelid::regclass AS tbl
                             , attname            AS col
                             , atttypid::regtype  AS datatype
                               -- more attributes?
                        FROM   pg_attribute
                        WHERE  attrelid = '{}'::regclass  -- table name, optionally schema-qualified
                        AND    attnum > 0
                        AND    NOT attisdropped
                        ORDER  BY attnum;""".format(table_name)

    columns = []
    try:
        cursor.execute(col_names_query)
        col_names = cursor.fetchall()

        columns = [tup[1] for tup in col_names]

        cursor.close()

    except Exception as err:
        print("get_columns ERROR:", err)

    return columns


def get_rows(connection, table_name):
    cursor = connection.cursor()
    select_query = "SELECT * FROM {}".format(table_name)

    rows = []
    try:
        cursor.execute(select_query)
        rows = cursor.fetchall()

        cursor.close()
    except Exception as err:
        print("get_rows ERROR:", err)

    return rows


def get_data(table_name):
    try:
        # declare a new PostgreSQL connection object
        conn = connect(database="dvdrental", user="postgres",
                       password="password", host="127.0.0.1", port="5433")

        # print the connection if successful
        print("psycopg2 connection:", conn)

    except Exception as err:
        print("psycopg2 connect() ERROR:", err)
        conn = None

    data = []
    if conn is not None:
        columns = get_columns(conn, table_name)
        rows = get_rows(conn, table_name)

        for row in rows:
            relation = {}
            for i in range(len(columns)):
                relation[columns[i]] = str(row[i])
            data.append(relation)

    return data

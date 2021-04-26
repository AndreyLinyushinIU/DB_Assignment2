import json

from psycopg2 import connect
from config import postgres

tables = ['actor', 'address', 'category', 'city', 'country', 'customer', 'film', 'film_actor', 'film_category',
          'inventory', 'language', 'payment', 'rental', 'staff', 'store']


# define a function that gets the column names from a PostgreSQL table
def get_columns(connection, table_name):
    cursor = connection.cursor()

    col_names_query = """SELECT attname
                        FROM   pg_attribute
                        WHERE  attrelid = '{}'::regclass
                        AND    attnum > 0
                        AND    NOT attisdropped
                        ORDER  BY attnum;""".format(table_name)

    columns = []
    try:
        cursor.execute(col_names_query)
        col_names = cursor.fetchall()

        columns = [tup[0] for tup in col_names]

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
        conn = connect(database=postgres['database'], user=postgres['username'],
                       password=postgres['password'], host=postgres['host'], port=postgres['port'])

        # print the connection if successful
        print("psycopg2 connection:", conn)

    except Exception as err:
        print("psycopg2 connect() ERROR:", err)
        conn = None

    data = {}
    if conn is not None:
        columns = get_columns(conn, table_name)
        rows = get_rows(conn, table_name)

        for i, row in enumerate(rows):
            relation = {columns[j]: str(row[j]) for j in range(len(columns))}
            data['record'+str(i)] = relation

    if len(data) > 0:
        print("table " + table_name + " retrieved successfully")
    return data


def get_json(filename):
    data = {table_name: get_data(table_name) for table_name in tables}

    with open(filename, "w") as write_file:
        json.dump(data, write_file)

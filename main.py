import pyrebase
import config
import psql_input

DB = pyrebase.initialize_app(config.firebase).database()


# psql_name table becomes transferred to firebase table firebase_name
def add_table(psql_name, firebase_name):
    data = psql_input.get_data(psql_name)
    for i in range(len(data)):
        DB.child(firebase_name).child('record'+str(i)).set(data[i])


# entry
if __name__ == '__main__':
    add_table('actor', 'new_actor')
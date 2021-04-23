# imports for using firebase
import pyrebase
import config
import psql_input
import threading
import datetime
import math

# imports for using firestore
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

# firebase
DB = pyrebase.initialize_app(config.firebase).database()

# firestore
cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)
db = firestore.client()


# psql_name table becomes transferred to firebase table firebase_name
def add_table_realtime(table_name):
    data = psql_input.get_data(table_name)
    for i in range(len(data)):
        DB.child(table_name).child('record' + str(i)).set(data[i])


# adding all tables
def add_realtime_database(list_of_tables):
    for table_name in list_of_tables:
        threading.Thread(target=add_table, args=(table_name,)).start()


def add_table_firestore(table_name):
    data = psql_input.get_data(table_name)
    counter = math.ceil(len(data) / 500)
    for i in range(counter):
        batch = db.batch()
        right_border = min((i + 1) * 500, len(data))
        for rec in data[i * 500:right_border]:
            doc = db.collection(table_name).document()
            batch.set(doc, rec)
        batch.commit()


def add_firestore(list_of_tables):
    for table_name in list_of_tables:
        threading.Thread(target=add_table_firestore, args=(table_name,)).start()


def query1():
    payments = DB.child('payment').get()
    count_smaller_pay = {}
    for payment in payments.each():
        if payment.val()['amount'] in count_smaller_pay.keys():
            count_smaller_pay[payment.val()['amount']] += 1
        else:
            count_smaller_pay[payment.val()['amount']] = 0
    print(count_smaller_pay)
    print(5 * '-----------------------------------------------------------------------------------------------------\n')
    rentals = DB.child('rental').get()
    ordered_payments_by_rental = DB.child('payment').order_by_child('rental_id')
    for rental in rentals.each():
        payments_set = ordered_payments_by_rental.equal_to(rental.val()['rental_id']).get()
        for el in payments_set.each():
            print(rental.val() + '\n' + el.val() + '\n' + str(count_smaller_pay[int(el.val()['amount'])]) + '\n')


def query2read():
    actor_with_some_name = DB.child('actor').order_by_child('name').equal_to('some_name').get()
    print("Test read: ")
    print(actor.val() + '\n' for actor in actor_with_some_name.each())


def query2delete():
    actors = DB.child("actor").get()
    for actor in actors.each():
        if actor.val()['name'] == 'some_name':
            DB.child('actor').child(actor.key()).remove()


def query2update():
    actors = DB.child("actor").get()
    for actor in actors.each():
        if actor.val()['name'] == 'some_name':
            DB.child('actor').child(actor.key()).update({'secret_field': 'enabled'})


def setTime(data):
    print(f"check {data['path']}")
    if data['path'] != "/" and data['path'].count('/') == 2:
        time = datetime.datetime.now()
        refl = DB.child(data['path'])
        print(f"path {refl.path}")
        refl.update({'last_update':
                         f"{time.year}-{time.month}-{time.day} {time.hour}:{time.minute}:{time.second}.{time.microsecond}"})


if __name__ == '__main__':
    # listener = DB.stream(setTime)
    # add_tables(['actor', 'address', 'category', 'city', 'country', 'customer', 'film', 'film_actor', 'film_category',
    #            'inventory', 'language', 'payment', 'rental', 'staff', 'store'])
    # listener.close()
    add_firestore(['actor', 'address', 'category', 'city', 'country', 'customer', 'film', 'film_actor', 'film_category',
                'inventory', 'language', 'payment', 'rental', 'staff', 'store'])

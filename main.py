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


def add_table_realtime(table_name):
    data = psql_input.get_data(table_name)
    for i in range(len(data)):
        DB.child(table_name).child('record' + str(i)).set(data[i])
def add_realtime_database(list_of_tables):  #parallel execution of several tables
    for table_name in list_of_tables:
        threading.Thread(target=add_table_realtime, args=(table_name,)).start()


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
def add_firestore(list_of_tables):  #parallel execution of several tables
    for table_name in list_of_tables:
        threading.Thread(target=add_table_firestore, args=(table_name,)).start()


def query1():
    payments = DB.child('payment').get()
    ordered_rentals = DB.sort(DB.child('rental').get(), 'rental_id').each()
    ordered_payments = DB.sort(payments, 'rental_id').each()

    pay = {}
    for payment in payments.each():
        if payment.val()['amount'] in pay.keys():
            pay[payment.val()['amount']] += 1
        else:
            pay[payment.val()['amount']] = 1

    res = sorted(pay.items(), key = lambda p: float(p[0]))
    res = [res[0]] + [(res[i][0], res[i][1] + res[i-1][1]) for i in range(1, len(res))]
    res = [(res[i][0], res[i][1] - pay[res[i][0]]) for i in range(0, len(res))]

    pay.update({i[0]: i[1] for i in res})
    output = []

    pointer = 0
    for r in ordered_rentals:
        while pointer < len(ordered_payments) and ordered_payments[pointer].val()['rental_id'] < r.val()['rental_id']:
            pointer += 1
        while pointer < len(ordered_payments) and ordered_payments[pointer].val()['rental_id'] == r.val()['rental_id']:
            output.append(str(r.val()) + ' ' + str(ordered_payments[pointer].val()) + ' ' + str(pay[ordered_payments[pointer].val()['amount']]))
            pointer += 1
    return output


def query2insert():
    for i in range(5):
        DB.child('test_table').child('record' + str(i)).set({'data': 'important_data', 'value': i})


def query2delete():
    records = DB.child('test_table').get()
    for r in records.each():
        if r.val()['value'] == 3:
            DB.child('test_table').child(r.key()).remove()


def query2update():
    records = DB.child('test_table').get()
    for r in records.each():
        if r.val()['value'] == 4:
            DB.child('test_table').child(r.key()).update({'value': 100})


def setTime(data):
    print(f"check {data['path']}")
    if data['path'] != "/" and data['path'].count('/') == 2:
        time = datetime.datetime.now()
        refl = DB.child(data['path'])
        print(f"path {refl.path}")
        refl.update({'last_update':
                         f"{time.year}-{time.month}-{time.day} {time.hour}:{time.minute}:{time.second}.{time.microsecond}"})


if __name__ == '__main__':
    start = datetime.datetime.now()
    query2insert()
    query2delete()
    query2update()
    #print(query1())
    # listener = DB.stream(setTime)
    # add_realtime_database(['actor', 'address', 'category', 'city', 'country', 'customer', 'film', 'film_actor', 'film_category',
    #            'inventory', 'language', 'payment', 'rental', 'staff', 'store'])
    # listener.close()
    #add_firestore(['actor', 'address', 'category', 'city', 'country', 'customer', 'film', 'film_actor', 'film_category',
    #            'inventory', 'language', 'payment', 'rental', 'staff', 'store'])
    print(datetime.datetime.now()-start)
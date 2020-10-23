import time
import datetime
from timeit import default_timer as timer
import settings
from pymongo import MongoClient
from faker import Faker
from bson.decimal128 import Decimal128
import random

fake = Faker()

####
# Start script
####
startTs = time.gmtime()
start = timer()
print("================================")
print("   Generating Transactions Data ")
print("================================")
print("\nStarting " + time.strftime("%Y-%m-%d %H:%M:%S", startTs) + "\n")


####
# Main start function
####
def main():
    mongo_client = MongoClient(MDB_CONNECTION)
    db = mongo_client[MDB_DATABASE]
    my_collection = db[MDB_COLLECTION]

    print('Begin generating txns documents.')
    print('Number of documents to generate: ' + str(NUM_DOCS))

    for index in range(int(NUM_DOCS)):
        fake_timestamp = fake.date_between(start_date='-1y', end_date='today')
        txn_types = ['deposit', 'withdrawal']
        txns = random.choice(txn_types)

        my_txn_document = {
            "customerId": fake.ssn(),
            "name": fake.name(),
            "address": fake.street_address(),
            "city": fake.city(),
            "state": fake.state(),
            "postalCode": fake.postcode(),
            "email": fake.email(),
            "lastLocation": {
                "type": "Point",
                "coordinates": [
                    Decimal128(fake.longitude()),
                    Decimal128(fake.latitude())
                ]
            },
            "txnType": txns,
            "txnAmount": random.randint(0, 10000),
            "txnDate": datetime.datetime(fake_timestamp.year, fake_timestamp.month, fake_timestamp.day)
        }

        # Print example doc on first doc creation
        if index == 1:
            print('Example Document')
            print(my_txn_document)

        # Indicate how many docs inserted
        if index % 100 == 0:
            print('Docs inserted: ' + str(index))

        my_collection.insert_one(my_txn_document)


####
# Constants loaded from .env file
####
MDB_CONNECTION = settings.MDB_CONNECTION
MDB_DATABASE = settings.MDB_DATABASE
MDB_COLLECTION = settings.MDB_COLLECTION
NUM_DOCS = settings.NUM_DOCS

####
# Main
####
if __name__ == '__main__':
    main()

####
# Indicate end of script
####
end = timer()
endTs = time.gmtime()
total_time = end - start

if total_time < 1:
    docs_inserted_time = int(NUM_DOCS) / 1
else:
    docs_inserted_time = int(NUM_DOCS) / total_time

print("\nEnding " + time.strftime("%Y-%m-%d %H:%M:%S", endTs))
print('===============================')
print('Total Time Elapsed (in seconds): ' + str(total_time))
print('===============================')
print('Number of Docs inserted per second: ' + str(docs_inserted_time))
print('===============================')

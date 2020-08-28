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
print("   Generating Financial Data    ")
print("================================")
print("\nStarting " + time.strftime("%Y-%m-%d %H:%M:%S", startTs) + "\n")


####
# Main start function
####
def main():
    mongo_client = MongoClient(MDB_CONNECTION)
    db = mongo_client[MDB_DATABASE]
    my_collection = db[MDB_COLLECTION]

    print('Begin generating financial documents.')
    print('Number of documents to generate: ' + str(NUM_DOCS))

    for index in range(int(NUM_DOCS)):
        # create timestamp
        fake_timestamp = fake.date_between(start_date='-20y', end_date='today')

        my_financial_document = {
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
            "customerSinceDate": datetime.datetime(fake_timestamp.year, fake_timestamp.month, fake_timestamp.day)
        }

        # Give each either or both a checking account and a savings account
        choices = random.randint(1, 3)
        my_financial_document['accounts'] = []

        if choices in [1, 3]:
            my_financial_document['accounts'].append({
                'accountNum': str(random.randint(10000000, 44444444)),
                'accountType': 'checking',
                'balance': random.randint(-1000, 10000)
            })

        if choices in [2, 3]:
            my_financial_document['accounts'].append({
                'accountNum': str(random.randint(55555555, 88888888)),
                'accountType': 'savings',
                'interestRate': random.uniform(1.1, 4.9),
                'balance': random.randint(0, 99999)
            })

        # Print example doc on first doc creation
        if index == 1:
            print('Example Document')
            print(my_financial_document)

        # For every 10 documents, add an additional field
        if index % 10 == 0:
            my_financial_document.update({
                'previousBank': fake.company()
            })

        # Indicate how many docs inserted
        if index % 100 == 0:
            print('Docs inserted: ' + str(index))

        my_collection.insert_one(my_financial_document)


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

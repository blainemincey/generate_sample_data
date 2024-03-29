import time
import datetime
from timeit import default_timer as timer
import settings
from pymongo import MongoClient
from faker import Faker
from bson.decimal128 import Decimal128

fake = Faker()

####
# Start script
####
startTs = time.gmtime()
start = timer()
print("================================")
print("  Generating Sample Tower Data  ")
print("================================")
print("\nStarting " + time.strftime("%Y-%m-%d %H:%M:%S", startTs) + "\n")


####
# Main start function
####
def main():

    mongo_client = MongoClient(MDB_CONNECTION)
    db = mongo_client[MDB_DATABASE]
    my_collection = db[MDB_COLLECTION]

    print('Delete existing documents.')
    result = my_collection.delete_many({})
    print('Num docs deleted: ' + str(result.deleted_count))

    print('Begin generating Sample Tower/IOT Documents.')
    print('Number of documents to generate: ' + str(NUM_DOCS))

    for index in range(int(NUM_DOCS)):
        # create timestamp
        fake_timestamp = fake.date_this_year()

        # Define IoT Document
        my_iot_document = {
            "username": fake.user_name(),
            "remote_ipv4": fake.ipv4(),
            "httpMethod": fake.http_method(),
            "hostName": fake.hostname(),
            "portNum": fake.port_number(),
            "location": {
                "type": "Point",
                "coordinates": [
                    Decimal128(fake.longitude()),
                    Decimal128(fake.latitude())
                ]
            },
            "dateAccessed": datetime.datetime(fake_timestamp.year, fake_timestamp.month, fake_timestamp.day)
        }

        if index == 1:
            print('Example Document')
            print(my_iot_document)

        # Indicate how many docs inserted every 100 iterations
        if index % 100 == 0:
            print('Docs inserted: ' + str(index))

        my_collection.insert_one(my_iot_document)


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

if total_time > 60:
    print('Total Time Elapsed (in minutes): ' + str(total_time/60))
else:
    print('Total Time Elapsed (in seconds): ' + str(total_time))
print('===============================')
print('Number of Docs inserted per second: ' + str(docs_inserted_time))
print('===============================')

import time
import datetime
from timeit import default_timer as timer
import settings
from pymongo import MongoClient
import random
from faker import Faker
from bson.decimal128 import Decimal128

faker = Faker()

####
# Start script
####
startTs = time.gmtime()
start = timer()
print("================================")
print(" Generating Tractor Data        ")
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

    print('Begin generating Tractor Data.')
    print('Number of documents to generate: ' + str(NUM_DOCS))

    for index in range(int(NUM_DOCS)):

        # details of tractor
        tractor_brands = ['Challenger', 'Fendt', 'Massey Ferguson', 'Valtra', 'Cumberland', 'Cimbria', 'White Planters']

        # get random choices
        tractor_brand = random.choice(tractor_brands)
        latitude = random.uniform(31.26458, 35.3811)
        longitude = random.uniform(-83.07944, -87.27667)

        # checkin_ts
        checkin_ts = faker.date_time_between(start_date="-1y", end_date='now')

        # tractor iot data
        tractor_document = {
            "tractor_brand": tractor_brand,
            "vin": faker.uuid4(),
            "ip": faker.ipv4_public(),
            "fuel_level": faker.random_int(min=1, max=99),
            "fuel_temp": faker.random_int(min=80, max=120),
            "fuel_usage_rate": faker.random_int(min=4, max=12),
            "outdoor_temp": faker.random_int(min=50, max=95),
            "hydraulic_temp": faker.random_int(min=90, max=100),
            "location": {
                "type": "Point",
                "coordinates": [
                    Decimal128(str(longitude)),
                    Decimal128(str(latitude))
                ]
            },
            "checkin_ts": datetime.datetime(checkin_ts.year, checkin_ts.month, checkin_ts.day, checkin_ts.hour,
                                            checkin_ts.minute, checkin_ts.second)
        }

        # print out the first sample document for example/spot check
        if index == 0:
            print('Example Document')
            print(tractor_document)

        # Indicate how many docs inserted every 100 iterations
        if index > 0:
            if index % 100 == 0:
                print('Docs inserted: ' + str(index))

        # insert document
        my_collection.insert_one(tractor_document)

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

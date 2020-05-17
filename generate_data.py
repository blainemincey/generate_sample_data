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
print("============================")
print("   Generating Sample Data   ")
print("============================")
print("\nStarting " + time.strftime("%Y-%m-%d %H:%M:%S", startTs) + "\n")


####
# Main start function
####
def main():
    print('NUM DOCS TO GENERATE: ' + str(NUM_DOCS))

    mongo_client = MongoClient(MDB_CONNECTION)
    db = mongo_client[MDB_DATABASE]
    my_collection = db[MDB_COLLECTION]

    for index in range(int(NUM_DOCS)):
        # Define Subscription Doc
        my_document = {
            "ApplicationID": fake.uuid4(),
            "UserID": fake.uuid4(),
            "Type": fake.random_int(1, 10),
            "Address": "MyDeviceAddr",
            "Units": fake.random_int(1, 10),
            "Culture": {
                "LanguageCode": fake.language_code(),
                "CountryCode": fake.country_code()
            },
            "Target": {
                "IsFollowMe": True,
                "LocationInfo": {
                    "Point": {
                        "type": "Point",
                        "coordinates": [
                            Decimal128(fake.longitude()),
                            Decimal128(fake.latitude())
                        ]
                    },
                    "PostalKey": fake.postcode(),
                    "CountryCode": fake.country_code(),
                    "TimeZone": fake.timezone(),
                    "DmaID": "524",
                    "CityID": fake.uuid4(),
                    "CountyID": "GAC057",
                    "PollenID": "ATL",
                    "TideID": "",
                    "ZoneID": "GAZ021",
                    "AdminDistrict": "GA:US"
                },
                "SavedLocationType": fake.random_int(1, 10)
            },
            "Schedule": {
                "IsActive": False,
                "Days": None,
                "Hours": None,
                "TimeZone": ""

            },
            "CreatedOn": datetime.datetime.now()
        }

        my_collection.insert_one(my_document)


####
# Constants (URL, SECRET, and NUM_REQUESTS loaded from .env file)
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
print("\nEnding " + time.strftime("%Y-%m-%d %H:%M:%S", endTs))
print('===============================')
print('Total Time Elapsed (in seconds): ' + str(end - start))
print('===============================')

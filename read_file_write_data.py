import time
from timeit import default_timer as timer
import settings
from pymongo import MongoClient
import json

####
# Start script
####
startTs = time.gmtime()
start = timer()
print("============================")
print("   Read File/Write Data     ")
print("============================")
print("\nStarting " + time.strftime("%Y-%m-%d %H:%M:%S", startTs) + "\n")


####
# Main start function
####
def main():
    mongo_client = MongoClient(MDB_CONNECTION)
    db = mongo_client[MDB_DATABASE]
    my_collection = db[MDB_COLLECTION]

    print("Begin reading JSON file: " + JSON_FILE_TO_PARSE)
    with open(JSON_FILE_TO_PARSE, "r") as read_file:
        print("Converting JSON encoded data into Python dictionary")
        my_json_file = json.load(read_file)

        counter = 0

        print("Decoded JSON Data From File")
        for document in my_json_file:
            print(document)
            my_collection.insert_one(document)
            counter = counter + 1

        print("Done reading json file")

        print("Number of JSON documents written to MongoDB: " + str(counter))

####
# Constants (URL, SECRET, and NUM_REQUESTS loaded from .env file)
####
MDB_CONNECTION = settings.MDB_CONNECTION
MDB_DATABASE = settings.MDB_DATABASE
MDB_COLLECTION = settings.MDB_COLLECTION
JSON_FILE_TO_PARSE = settings.JSON_FILE_TO_PARSE

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

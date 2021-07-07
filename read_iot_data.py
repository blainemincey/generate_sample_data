import time
from timeit import default_timer as timer
import settings
from pymongo import MongoClient
import asyncio
import time
import settings
import sys
import os
from pymongo import MongoClient
from threading import Thread
import random

####
# Start script
####
startTs = time.gmtime()
start = timer()
print("================================")
print("       Read IoT Data            ")
print("================================")
print("\nStarting " + time.strftime("%Y-%m-%d %H:%M:%S", startTs) + "\n")


####
# Main start function
####
def main():
    # Create first read thread
    print('Start first thread.')
    read_1_thread = asyncio.new_event_loop()
    read_1_thread.call_soon_threadsafe(read_1)
    t = Thread(target=start_loop, args=(read_1_thread,))
    t.start()
    time.sleep(0.25)

    # Create second read thread
    print('Start second thread.')
    read_2_thread = asyncio.new_event_loop()
    read_2_thread.call_soon_threadsafe(read_2)
    t = Thread(target=start_loop, args=(read_2_thread,))
    t.start()
    time.sleep(0.25)

    # Create third read thread
    print('Start third thread.')
    read_3_thread = asyncio.new_event_loop()
    read_3_thread.call_soon_threadsafe(read_3)
    t = Thread(target=start_loop, args=(read_3_thread,))
    t.start()
    time.sleep(0.25)

    # Create fourth read thread
    print('Start fourth thread.')
    read_4_thread = asyncio.new_event_loop()
    read_4_thread.call_soon_threadsafe(read_4)
    t = Thread(target=start_loop, args=(read_4_thread,))
    t.start()
    time.sleep(0.25)

    # Create fifth read thread
    print('Start fifth thread.')
    read_5_thread = asyncio.new_event_loop()
    read_5_thread.call_soon_threadsafe(read_5)
    t = Thread(target=start_loop, args=(read_5_thread,))
    t.start()
    time.sleep(0.25)


def read_1():
    mongo_client = MongoClient(MDB_CONNECTION)
    db = mongo_client[MDB_DATABASE]
    my_collection = db[MDB_COLLECTION]

    for index in range(int(NUM_DOCS)):
        iot_document = my_collection.find_one({"index": random.randint(0, 10000)})
        print('Read 1 Thread Result:')
        print(iot_document)


def read_2():
    mongo_client = MongoClient(MDB_CONNECTION)
    db = mongo_client[MDB_DATABASE]
    my_collection = db[MDB_COLLECTION]

    for index in range(int(NUM_DOCS)):
        iot_document = my_collection.find_one({"index": random.randint(0, 10000)})
        print('Read 2 Thread Result:')
        print(iot_document)


def read_3():
    mongo_client = MongoClient(MDB_CONNECTION)
    db = mongo_client[MDB_DATABASE]
    my_collection = db[MDB_COLLECTION]

    for index in range(int(NUM_DOCS)):
        iot_document = my_collection.find_one({"index": random.randint(0, 10000)})
        print('Read 3 Thread Result:')
        print(iot_document)


def read_4():
    mongo_client = MongoClient(MDB_CONNECTION)
    db = mongo_client[MDB_DATABASE]
    my_collection = db[MDB_COLLECTION]

    for index in range(int(NUM_DOCS)):
        iot_document = my_collection.find_one({"index": random.randint(0, 10000)})
        print('Read 4 Thread Result:')
        print(iot_document)


def read_5():
    mongo_client = MongoClient(MDB_CONNECTION)
    db = mongo_client[MDB_DATABASE]
    my_collection = db[MDB_COLLECTION]

    for index in range(int(NUM_DOCS)):
        iot_document = my_collection.find_one({"index": random.randint(0, 10000)})
        print('Read 5 Thread Result:')
        print(iot_document)


####
# Make sure the loop continues
####
def start_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


####
# Constants loaded from .env file
####
#MDB_CONNECTION = settings.MDB_CONNECTION
MDB_CONNECTION = settings.MDB_ANALYTICS_CONNECTION
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

print("\nEnding " + time.strftime("%Y-%m-%d %H:%M:%S", endTs))
print('===============================')

import time
from datetime import date, datetime, timedelta
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
print("==========================================")
print("   Testing MongoDB Atlas Online Archive   ")
print("==========================================")
print("\nStarting " + time.strftime("%Y-%m-%d %H:%M:%S", startTs) + "\n")


####
# Main start function
####
def main():
    thirty_days_ago = date.today() - timedelta(days=30)
    thirty_days_ago_dt = datetime(thirty_days_ago.year, thirty_days_ago.month, thirty_days_ago.day)
    print('Date 30 days ago: ' + str(thirty_days_ago_dt))

    # Hot data in MongoDB Atlas
    mongo_client = MongoClient(MDB_CONNECTION)
    db = mongo_client[MDB_DATABASE]
    my_collection = db[MDB_COLLECTION]

    print('Number of Documents in Atlas Collection')
    print('--> ' + str(my_collection.count_documents({})))

    print('Number of Documents in Atlas Collection to be archived (older than 30 days)')
    print('--> ' + str(my_collection.count_documents({'dateAccessed': {'$lt': thirty_days_ago_dt}})))

    # Federated Query Connection
    mongo_client_oa_fed = MongoClient(MDB_OA_FEDERATED_CONNECTION)
    oa_fed_db = mongo_client_oa_fed[MDB_DATABASE]
    oa_fed_col = oa_fed_db[MDB_COLLECTION]

    print('Federated Query - Number of Documents across Atlas and Online Archive')
    print('--> ' + str(oa_fed_col.count_documents({})))

    # Group and Count in Aggregation
    pipeline = [
        {
            '$group': {
                '_id': '$httpMethod',
                'count': {
                    '$sum': 1
                }
            }
        }, {
            '$sort': {
                'count': -1
            }
        }
    ]

    # Federated Aggregation
    print('\nAtlas - Group by HTTP Method, Count of Num of Documents')
    cursor = list(my_collection.aggregate(pipeline))
    for idx in cursor:
        print(idx)

    # Federated Aggregation
    print('\nAtlas + Online Archive - Group by HTTP Method, Count of Num of Documents')
    fed_cursor = list(oa_fed_col.aggregate(pipeline))
    for fed_idx in fed_cursor:
        print(fed_idx)

    # close connections
    mongo_client_oa_fed.close()
    mongo_client.close()


####
# Constants loaded from .env file
####
MDB_CONNECTION = settings.MDB_CONNECTION
MDB_OA_FEDERATED_CONNECTION = settings.MDB_OA_FEDERATED_CONNECTION
MDB_DATABASE = settings.MDB_DATABASE
MDB_COLLECTION = settings.MDB_COLLECTION

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

#!/usr/bin/env python3
import os
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

# Accessing variables.
MDB_CONNECTION = os.getenv('MDB_CONNECTION')
MDB_DATABASE = os.getenv('MDB_DATABASE')
MDB_COLLECTION = os.getenv('MDB_COLLECTION')
NUM_DOCS = os.getenv('NUM_DOCS')
MDB_OA_FEDERATED_CONNECTION = os.getenv('MDB_OA_FEDERATED_CONNECTION')
JSON_FILE_TO_PARSE = os.getenv('JSON_FILE_TO_PARSE')

print("Settings loaded from .env file.")

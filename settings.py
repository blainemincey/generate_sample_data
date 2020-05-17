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

print("Settings loaded from .env file.")

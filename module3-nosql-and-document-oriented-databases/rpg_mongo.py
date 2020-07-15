"""
How was working with MongoDB different from working with PostgreSQL?

Working with MongoDB was harder because of the diferent syntax than what I'm used to. 

Moreover, it has more freedom for loading data. Which could be a good thing, but also adds more changes for human error. 

I'll research different use cases for NoSQL and SQL databases. 
"""


# app/mongo_queries.py

import pymongo
import os
import sqlite3
from dotenv import load_dotenv
import json

load_dotenv()

DB_USER = os.getenv("MONGO_USER2", default="OOPS")
DB_PASSWORD = os.getenv("MONGO_PASSWORD2", default="OOPS")
CLUSTER_NAME = os.getenv("MONGO_CLUSTER_NAME2", default="OOPS")

# CONNECTING WITH THE MONGO DB

connection_uri = f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@{CLUSTER_NAME}.mongodb.net/test?retryWrites=true&w=majority"
print("----------------")
print("URI:", connection_uri)

client = pymongo.MongoClient(connection_uri)
print("----------------")
print("CLIENT:", type(client), client)
print("DATABASES:", client.list_database_names)

db = client.rpg_db # "rpg_db" or whatever you want to call the DB
print("----------------")
print("DB:", type(db), db)
print("COLLECTIONS:", db.list_collection_names())

collection = db.armory_weapon # "charactercreator_character" or whatever you want to call the table
print("----------------")
print("COLLECTION:", type(collection), collection)
print("DOCUMENTS COUNT (ROWS):", collection.count_documents({}))


# GETTING THE DATA FROM THE RPG DB 

DB_FILEPATH = os.path.join(os.path.dirname(__file__), "..", "module1-introduction-to-sql", "rpg_db.sqlite3")

connection = sqlite3.connect(DB_FILEPATH)
connection.row_factory = sqlite3.Row
cursor = connection.cursor()

cursor.execute("SELECT * FROM armory_weapon;")
column_names = [d[0] for d in cursor.description]

# PREPARING THE DATA
characters = []
for row in cursor:
    info = dict(zip(column_names, row))
    characters.append(info)

# INSERTING THE DATA INTO THE MONGO DB
collection.insert_many(characters)
print("DOCUMENTS (ROWS) COUNT AFTER INSERTED DATA:", collection.count_documents({}))




# DB_URL = "mongodb+srv://user123:<password>@cluster0.gpndx.mongodb.net/<dbname>?retryWrites=true&w=majority"



# app/mongo_queries.py

import pymongo
import os
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("MONGO_USER", default="OOPS")
DB_PASSWORD = os.getenv("MONGO_PASSWORD", default="OOPS")
CLUSTER_NAME = os.getenv("MONGO_CLUSTER_NAME", default="OOPS")

connection_uri = f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@{CLUSTER_NAME}.mongodb.net/test?retryWrites=true&w=majority"
print("----------------")
print("URI:", connection_uri)

client = pymongo.MongoClient(connection_uri)
print("----------------")
print("CLIENT:", type(client), client)
print("DATABASES:", client.list_database_names())


db = client.test_database # "test_database" or whatever you want to call it
print("----------------")
print("DB:", type(db), db)
print("COLLECTIONS", db.list_collection_names())

collection = db.pokemon_test # "pokemon_test" or whatever you want to call it
print("----------------")
print("COLLECTION:", type(collection), collection)

print("DOCUMENTS COUNT:", collection.count_documents({}))



for doc in collection.find({"level": {"$gt": 10}}):
    print(doc)

exit()

# INSERT ONE
collection.insert_one({
    "name": "Pikachu",
    "level": 30,
    "exp": 76000000000,
    "hp": 400,
    "parents": ["Pikachu A", "Raichu"],
    "other_attr": {
        "a":1,
        "b":2,
        "c":3
    }
})
print("DOCUMENTS COUNT:", collection.count_documents({}))
print(collection.count_documents({"name": "Pikachu"}))


# INSERT MANY

bulbasaur = {
    "name": "Bulbasaur",
    "type": "grass",
    "moves":["Leech Seed", "Solar Beam"]
}
eevee = {
    "name": "eevee",
    "hp": 70,
    "moves":["Leech Seed", "Solar Beam"]
}
chansey = {
    "name": "chansey",
    "type": "Flair",
    "moves":["Leech Seed", "Solar Beam"]
}

snorlax = {}

team = [bulbasaur, eevee, chansey,snorlax]

collection.insert_many(team)

print("DOCUMENTS COUNT:", collection.count_documents({}))





# ------

# breakpoint()

# dir(collection) <- let us know what methos are available to call on the object
# #from pprint import pprint <- to better see the methods
# pprint(dir(collection))



import pandas as pd 
import os
import sqlite3

df = pd.read_csv("module1-introduction-to-sql/buddymove_holidayiq.csv")
df = df.rename(columns={'User Id':'id'})

# construct a path to wherever your database exists
DB_FILEPATH = os.path.join(os.path.dirname(__file__), "buddymove_holidayiq.sqlite3")

connection = sqlite3.connect(DB_FILEPATH)
print("CONNECTION:", connection)

cursor = connection.cursor()
print("CURSOR", cursor)

# Transform the DF into SQL
#df.to_sql('review', con=connection, if_exists='replace')

# Query 1: Count how many rows you have
query = "SELECT count(DISTINCT id) as user_count FROM review;"

result1 = cursor.execute(query).fetchone()
print("RESULT 1: Count how many rows you have", result1)

# Query 2: How many users who reviewed at least 100 Nature in the category 
# also reviewed at least 100 in the Shopping category?
query = "SELECT count(DISTINCT id) as count FROM review \
	WHERE Nature >= 100 and	Shopping >= 100;"

result2 = cursor.execute(query).fetchone()
print("RESULT 2: How many users who reviewed at least 100 Nature in the category \
    also reviewed at least 100 in the Shopping category?", result2)
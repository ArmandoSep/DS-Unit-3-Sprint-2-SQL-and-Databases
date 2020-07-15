import os
import pandas
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
import numpy as np

# to get over errors about not being able to work with the numpy integer datatypes
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

load_dotenv() #> loads contents of the .env file into the script's environment

DB_NAME = os.getenv("DB_NAME2")
DB_USER = os.getenv("DB_USER2")
DB_PASSWORD = os.getenv("DB_PASSWORD2")
DB_HOST = os.getenv("DB_HOST2")

# READ CSV
CVS_FILEPATH = os.path.join(os.path.dirname(__file__), "titanic.csv")
df = pandas.read_csv(CVS_FILEPATH)
df.index += 1 # to start index at 1 (resembling primary key behavior)
print(df.head())

# CONNECT TO PG DATABASE
connection = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)
print(type(connection)) #> <class 'psycopg2.extensions.connection'>

cursor = connection.cursor()
print(type(cursor)) #> <class 'psycopg2.extensions.cursor'>

# CREATE TABLE
table_creation_query = """
DROP TABLE passengers;
CREATE TABLE IF NOT EXISTS passengers (
  id SERIAL PRIMARY KEY,
  survived integer,
  pclass integer,
  name varchar NOT NULL,
  gender varchar NOT NULL,
  age float,
  sib_spouse_count integer,
  parent_child_count integer,
  fare float
);
"""
cursor.execute(table_creation_query)


#INSERT DATA IN THE TABLE
list_of_tuples = list(df.to_records(index=True))
# possibly sometimes would need to do further transformations, perhaps using a list comprehension or something

insertion_query = "INSERT INTO passengers (id, survived, pclass, name, gender, age, sib_spouse_count, parent_child_count, fare) VALUES %s"
execute_values(cursor, insertion_query, list_of_tuples)


# QUERIES

# Query 1: How many passengers survived, and how many died?
query = "SELECT count(id) as count, survived = 1 as survived from passengers GROUP BY survived;"
result1 = cursor.execute(query)
result1 = cursor.fetchall()
print("RESULT 1: How many passengers survived, and how many died?", result1)

# Query 2: How many passengers were in each class?
query = "SELECT count(id) as count, pclass from passengers GROUP BY pclass;"
result2 = cursor.execute(query)
result2 = cursor.fetchall()
print("RESULT 2: How many passengers were in each class?", result2)

# Query 3: How many passengers survived/died within each class?
query = "SELECT pclass, survived = 1 as survived, count(*) as count FROM passengers GROUP by survived, pclass ORDER BY pclass;"
result3 = cursor.execute(query)
result3 = cursor.fetchall()
print("RESULT 3: How many passengers survived/died within each class?", result3)

# Query 4: What was the average age of survivors vs nonsurvivors?
query = "SELECT	survived = 1 as survived,	count(*) as count, avg(age) as average_age FROM passengers GROUP by survived;"
result4 = cursor.execute(query)
result4 = cursor.fetchall()
print("RESULT 4: What was the average age of survivors vs nonsurvivors?", result4)

# Query 5: What was the average age of each passenger class?
query = "SELECT pclass,	avg(age) as average_age FROM passengers GROUP by pclass ORDER BY pclass;"
result5 = cursor.execute(query)
result5 = cursor.fetchall()
print("RESULT 5: What was the average age of each passenger class?", result5)

# Query 6: What was the average fare by passenger class? By survival?
query = """
SELECT 
	pclass,
	survived = 1 as survived,
	count(*) as count,
	avg(age) as average_age
FROM passengers
GROUP by survived, pclass 
ORDER BY pclass;
"""
result6 = cursor.execute(query)
result6 = cursor.fetchall()
print("RESULT 6: What was the average fare by passenger class? By survival?", result6)

# Query 7: How many siblings/spouses aboard on average, by passenger class? By survival?
query = """
SELECT 
	pclass,
	survived = 1 as survived,
	count(*) as count,
	round(avg(sib_spouse_count), 2) as average_sibspouse
FROM passengers
GROUP by survived, pclass 
ORDER BY pclass
"""
result7 = cursor.execute(query)
result7 = cursor.fetchall()
print("RESULT 7: How many siblings/spouses aboard on average, by passenger class? By survival?", result7)

# Query 8: How many parents/children aboard on average, by passenger class? By survival?
query = """
SELECT 
	pclass,
	survived = 1 as survived,
	count(*) as count,
	round(avg(parent_child_count), 2) as average_children
FROM passengers
GROUP by survived, pclass 
ORDER BY pclass
"""
result8 = cursor.execute(query)
result8 = cursor.fetchall()
print("RESULT 8: How many parents/children aboard on average, by passenger class? By survival?", result8)

# Query 9: Do any passengers have the same first name?
query = "SELECT count(id) as count,	split_part(name, ' ', 2) first_name FROM passengers GROUP BY first_name ORDER BY count DESC;"
result9 = cursor.execute(query)
result9 = cursor.fetchall()
print("RESULT 9: Do any passengers have the same first name?", result9)


# SAVE THE RESULTS!
connection.commit()
# CLEAN UP!
cursor.close()
connection.close()
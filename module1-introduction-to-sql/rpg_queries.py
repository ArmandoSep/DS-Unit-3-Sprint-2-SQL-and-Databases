import os
import sqlite3

# construct a path to wherever your database exists
# DB_FILEPATH = "module1-introduction-to-sql/chinook.db"
# DB_FILEPATH = os.path.join(os.path.dirname(__file__), "..", "chinook.db") # ".." is to go up one folder
DB_FILEPATH = os.path.join(os.path.dirname(__file__), "rpg_db.sqlite3")


connection = sqlite3.connect(DB_FILEPATH)
print("CONNECTION:", connection)

cursor = connection.cursor()
print("CURSOR", cursor)

# Query 1 - How many total Characters are there?
query = "SELECT count(DISTINCT character_id) as character_count FROM charactercreator_character;"

result1 = cursor.execute(query).fetchone()
print("RESULT 1: How many total Characters are there?", result1)

# Query 2 - How many of each specific subclass?
query = "SELECT count(distinct mg.character_ptr_id) as mage_count \
	,count(distinct tf.character_ptr_id) as thief_count \
	,count(distinct cl.character_ptr_id) as cleric_count \
	,count(distinct ft.character_ptr_id) as fighter_count \
FROM charactercreator_character ch \
LEFT JOIN charactercreator_mage mg ON ch.character_id = mg.character_ptr_id \
LEFT JOIN charactercreator_thief tf ON ch.character_id = tf.character_ptr_id \
LEFT JOIN charactercreator_cleric cl ON ch.character_id = cl.character_ptr_id \
LEFT JOIN charactercreator_fighter ft ON ch.character_id = ft.character_ptr_id;"

result2 = cursor.execute(query).fetchone()
print("RESULT 2: How many of each specific subclass?", result2)

# Query 3 - How many total Items?
query = "SELECT count(DISTINCT item_id) as item_count FROM armory_item;"

result3 = cursor.execute(query).fetchone()
print("RESULT 3: How many total Items?", result3)

# Query 4 - How many of the Items are weapons? How many are not?
query = "SELECT count(DISTINCT armory_weapon.item_ptr_id) as weapon_count \
	,count(armory_weapon.item_ptr_id is null) - count(DISTINCT armory_weapon.item_ptr_id) as non_weapon_count \
FROM armory_item \
LEFT JOIN armory_weapon ON armory_item.item_id = armory_weapon.item_ptr_id;"

result4 = cursor.execute(query).fetchone()
print("RESULT 4: How many of the Items are weapons? How many are not?", result4)

# Query 5 - How many Items does each character have? (Return first 20 rows)
query = "SELECT ch.character_id \
	,count(DISTINCT it.item_id) as item_count \
FROM charactercreator_character ch \
JOIN charactercreator_character_inventory it ON ch.character_id = it.character_id \
GROUP BY ch.character_id \
LIMIT 20;"

result5 = cursor.execute(query).fetchall()
print("RESULT 5: How many Items does each character have? (Return first 20 rows)", result5)

# Query 6 - How many Items does each character have? (Return first 20 rows)
query = "SELECT chin.character_id \
	,count(DISTINCT aw.item_ptr_id) as weapon_count \
FROM charactercreator_character_inventory chin \
JOIN armory_item it ON chin.item_id = it.item_id \
JOIN armory_weapon aw ON it.item_id = aw.item_ptr_id \
GROUP BY chin.character_id \
LIMIT 20;"

result6 = cursor.execute(query).fetchall()
print("RESULT 6: How many Items does each character have? (Return first 20 rows)", result6)

# Query 7 - On average, how many Items does each Characters have?
query = "SELECT \
	count(chin.item_id) / CAST(count(distinct chin.character_id) as float) as avg_ch_items \
FROM charactercreator_character_inventory chin;"

result7 = cursor.execute(query).fetchone()
print("RESULT 7: On average, how many Items does each Character have?", result7)

# Query 8 - On average, how many Weapons does each character have?
query = "SELECT CAST(count(aw.item_ptr_id) as float) / count(distinct chin.character_id) as avg_ch_weapons \
FROM charactercreator_character_inventory chin \
JOIN armory_item it ON chin.item_id = it.item_id \
LEFT JOIN armory_weapon aw ON it.item_id = aw.item_ptr_id;"

result8 = cursor.execute(query).fetchone()
print("RESULT 8: On average, how many Weapons does each character have?", result8)

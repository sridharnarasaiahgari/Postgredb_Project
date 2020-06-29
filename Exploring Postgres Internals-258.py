## 1. Introduction ##

import psycopg2
conn = psycopg2.connect(dbname='hud', user='hud_admin', password='eRqg123EEkl')
conn.close()

## 2. Investigating the Tables ##

import psycopg2
conn = psycopg2.connect(dbname='hud', user='hud_admin', password='eRqg123EEkl')
cur = conn.cursor()
cur.execute("""SELECT table_name FROM information_schema.tables ORDER BY table_name;""")
table_names = cur.fetchall()
print(len(table_names))
for name in table_names:
    print(name)


## 3. Working with Schemas ##

import psycopg2
conn = psycopg2.connect(dbname='hud', user='hud_admin', password='eRqg123EEkl')
cur = conn.cursor()
cur.execute("""
   SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'
   ORDER BY table_name;
""")
table_names = cur.fetchall()
print(len(table_names))
for name in table_names:
    print(name)

## 4. Describing the Tables ##

import psycopg2
from psycopg2.extensions import AsIs
conn = psycopg2.connect(dbname='hud', user='hud_admin', password='eRqg123EEkl')
cur = conn.cursor()
col_descriptions = {}
for table_name in table_names:
    cur.execute("SELECT * FROM %s LIMIT 0;", (AsIs(table_name),))
    col_descriptions[table_name] = cur.description
conn.close()

## 5. Type Code Mappings ##

import psycopg2
conn = psycopg2.connect(dbname='hud', user='hud_admin', password='eRqg123EEkl')
cur = conn.cursor()
type_mappings = {}
cur.execute("SELECT oid, typname FROM pg_catalog.pg_type")
all_types = cur.fetchall()
for typ in all_types:
    type_mappings[typ[0]] = typ[1]
print(type_mappings[1082])

## 6. Readable Description Types ##

# variables: table_names, type_mappings and col_descriptions from the previous screens are available
readable_description = {}
for table_name in col_descriptions:
    readable_description [table_name] = {
        "columns": [{"name": col.name,
                    "type": type_mappings[col.type_code],
                    "internal_size": col.internal_size
                    }
                   for col in col_descriptions[table_name]
                   ]
    }

## 7. Number of Rows ##

import psycopg2
from psycopg2.extensions import AsIs
conn = psycopg2.connect(dbname='hud', user='hud_admin', password='eRqg123EEkl')
cur = conn.cursor()
# variables: table_names and readable_description from the previous screens are available
for table_name in readable_description.keys():
    cur.execute("SELECT COUNT(*) FROM %s;", [AsIs(table_name)])
    readable_description[table_name]["number_of_rows"] = cur.fetchone()[0]
print(readable_description)

## 8. The JSON Format ##

# the variable readable_description is available for you
import json
#json_str = json.dumps(readable_description)
#json_str = json.dumps(readable_description, indent=4)
json_str = json.dumps(readable_description, indent=2)
print(json_str)
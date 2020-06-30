## 1. Alternate Table Scans ##

import psycopg2
import json
conn = psycopg2.connect(dbname='hud', user='hud_admin', password='eRqg123EEkl')
cur = conn.cursor()
cur.execute("""
    EXPLAIN (ANALYZE, FORMAT json) 
    SELECT * FROM homeless_by_coc
    WHERE id = 10;
""")
query_plan = cur.fetchone()
print(json.dumps(query_plan, indent=2))

## 2. Index Scan ##

import psycopg2
conn = psycopg2.connect(dbname='hud', user='hud_admin', password='eRqg123EEkl')
cur = conn.cursor()
cur.execute("""
    EXPLAIN (ANALYZE, FORMAT json) 
    SELECT * FROM homeless_by_coc 
    WHERE coc_name = 'Chester County CoC'
    LIMIT 1;
""")
coc_name_plan = cur.fetchone()
print(coc_name_plan[0][0]["Execution Time"])

cur.execute("""
    EXPLAIN (ANALYZE, FORMAT json) 
    SELECT * FROM homeless_by_coc 
    WHERE id = 42704;
""")
id_plan = cur.fetchone()
print(id_plan[0][0]["Execution Time"])

## 4. Indexing ##

import psycopg2
conn = psycopg2.connect(dbname='hud', user='hud_admin', password='eRqg123EEkl')
cur = conn.cursor()
cur.execute("""
    CREATE INDEX coc_name_index ON homeless_by_coc(coc_name);
""")
conn.commit()
conn.close()

## 5. Comparing the Queries ##

import psycopg2
import json
conn = psycopg2.connect(dbname='hud', user='hud_admin', password='eRqg123EEkl')
cur = conn.cursor()
cur.execute("""
    EXPLAIN (ANALYZE, FORMAT json) 
    SELECT * FROM homeless_by_coc 
    WHERE coc_name = 'Chester County CoC'
    LIMIT 1;
""")
coc_name_plan = cur.fetchone()
print(coc_name_plan[0][0]["Execution Time"])

## 6. The Indexes Table ##

import psycopg2
import json
conn = psycopg2.connect(dbname='hud', user='hud_admin', password='eRqg123EEkl')
cur = conn.cursor()
cur.execute("""
    SELECT * FROM pg_indexes
    WHERE tablename = 'homeless_by_coc';
""")
indexes = cur.fetchall()
for index in indexes:
    print(index)

## 7. Dropping Indexes ##

import psycopg2
import json
conn = psycopg2.connect(dbname='hud', user='hud_admin', password='eRqg123EEkl')
cur = conn.cursor()
cur.execute("""
    DROP INDEX IF EXISTS coc_name_index;
""")
conn.commit()
conn.close()

## 8. Index Performance on Joins ##

import psycopg2
import json
conn = psycopg2.connect(dbname='hud', user='hud_admin', password='eRqg123EEkl')
cur = conn.cursor()
cur.execute("""
   EXPLAIN (ANALYZE, FORMAT json)
   SELECT homeless_by_coc.state, homeless_by_coc.coc_number, homeless_by_coc.coc_name, state_info.name 
   FROM homeless_by_coc, state_info
   WHERE homeless_by_coc.state = state_info.postal;
""")
no_index_plan = cur.fetchone()
print(no_index_plan[0][0]["Execution Time"])

# create the index
cur.execute("CREATE INDEX state_index ON homeless_by_coc(state)")

# query with index
cur.execute("""
    EXPLAIN (ANALYZE, FORMAT json)
    SELECT homeless_by_coc.state, homeless_by_coc.coc_number, homeless_by_coc.coc_name, state_info.name 
    FROM homeless_by_coc, state_info
    WHERE homeless_by_coc.state = state_info.postal;
""")
index_plan = cur.fetchone()
print(index_plan[0][0]["Execution Time"])

conn.commit()

## 9. Understanding Index Performance on Joins ##

import psycopg2
import json
conn = psycopg2.connect(dbname='hud', user='hud_admin', password='eRqg123EEkl')
cur = conn.cursor()
cur.execute("""
   EXPLAIN (ANALYZE, FORMAT json)
   SELECT homeless_by_coc.state, homeless_by_coc.coc_number, homeless_by_coc.coc_name, state_info.name
   FROM homeless_by_coc
   INNER JOIN state_info
   ON homeless_by_coc.state = state_info.postal
   WHERE homeless_by_coc.count > 5000;
""")
no_index_plan = cur.fetchone()
print(no_index_plan[0][0]["Execution Time"])

cur.execute("CREATE INDEX count_index ON homeless_by_coc(count)")

cur.execute("""
    EXPLAIN (ANALYZE, FORMAT json)
    SELECT homeless_by_coc.state, homeless_by_coc.coc_number, homeless_by_coc.coc_name, state_info.name
    FROM homeless_by_coc
    INNER JOIN state_info
    ON homeless_by_coc.state = state_info.postal
    WHERE homeless_by_coc.count > 5000;
""")
index_plan = cur.fetchone()
print(index_plan[0][0]["Execution Time"])

conn.commit()
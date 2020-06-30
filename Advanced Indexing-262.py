## 1. Querying with Multiple Filters ##

import psycopg2
import json
conn = psycopg2.connect(dbname='hud', user='hud_admin', password='eRqg123EEkl')
cur = conn.cursor()
cur.execute("CREATE INDEX state_index ON homeless_by_coc(state);")
conn.commit()
cur.execute("""
    EXPLAIN (FORMAT json) 
    SELECT state FROM homeless_by_coc 
    WHERE state = 'CA' AND year < '2008-01-01';
""")
query_plan = cur.fetchone()
print(json.dumps(query_plan[0], indent=2))

## 3. Multi-Column Indexes ##

import psycopg2
import json
conn = psycopg2.connect(dbname='hud', user='hud_admin', password='eRqg123EEkl')
cur = conn.cursor()
cur.execute("""
    EXPLAIN (ANALYZE, FORMAT json) 
    SELECT * FROM homeless_by_coc 
    WHERE state='CA' AND year < '2008-01-01';
""")
plan_single_index = cur.fetchone()
print(plan_single_index[0][0]["Execution Time"])

cur.execute("CREATE INDEX state_year_index ON homeless_by_coc(state, year);")
conn.commit()

cur.execute("""
    EXPLAIN (ANALYZE, FORMAT json)
    SELECT * FROM homeless_by_coc
    WHERE state='CA' AND year < '2008-01-01';
""")
plan_multi_index = cur.fetchone()
print(plan_multi_index[0][0]["Execution Time"])

## 5. Index on More Than Two Columns ##

import psycopg2
import json
conn = psycopg2.connect(dbname='hud', user='hud_admin', password='eRqg123EEkl')
cur = conn.cursor()
cur.execute("CREATE INDEX state_count_year_index ON homeless_by_coc(state, count, year);")
conn.commit()
cur.execute("""
    EXPLAIN (ANALYZE, FORMAT json)
    SELECT * FROM homeless_by_coc
    WHERE year > '2011-01-01' AND count > 5000;
""")
query_plan = cur.fetchone()
print(json.dumps(query_plan, indent=2))

## 6. Index on Expressions ##

import psycopg2
conn = psycopg2.connect(dbname='hud', user='hud_admin', password='eRqg123EEkl')
cur = conn.cursor()
cur.execute("CREATE INDEX measures_index ON homeless_by_coc(LOWER(measures));")
conn.commit()
cur.execute("""
    SELECT * FROM homeless_by_coc 
    WHERE LOWER(measures) = 'total homeless';
""")
total_homeless = cur.fetchall()

## 7. Partial Indexes ##

import psycopg2
conn = psycopg2.connect(dbname='hud', user='hud_admin', password='eRqg123EEkl')
cur = conn.cursor()
cur.execute("CREATE INDEX partial_state_index ON homeless_by_coc(state) WHERE count = 0;")
conn.commit()
cur.execute("""
    SELECT * FROM homeless_by_coc 
    WHERE state  = 'CA' AND count = 0;
""")
ca_zero_count = cur.fetchall()

## 8. Building a Multi-Column Index ##

import psycopg2
conn = psycopg2.connect(dbname='hud', user='hud_admin', password='eRqg123EEkl')
cur = conn.cursor()
cur.execute("""
    CREATE INDEX state_year_measures_idx 
    ON homeless_by_coc(state, LOWER(measures)) 
    WHERE count = 0;
""")
conn.commit()
cur.execute("""
    EXPLAIN (ANALYZE, FORMAT json) 
    SELECT hbc.year, si.name, hbc.count
    FROM homeless_by_coc AS hbc
    INNER JOIN state_info AS si
    ON hbc.state = si.postal
    WHERE hbc.state = 'CA' AND hbc.count = 0 AND LOWER(hbc.measures) = 'total homeless';
""")
print(json.dumps(cur.fetchone(), indent=2))
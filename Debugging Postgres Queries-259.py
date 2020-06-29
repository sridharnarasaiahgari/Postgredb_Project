## 1. The EXPLAIN Query ##

import psycopg2
conn = psycopg2.connect(dbname='hud', user='hud_admin', password='eRqg123EEkl')
cur = conn.cursor()
cur.execute("""
    EXPLAIN 
    SELECT * FROM homeless_by_coc;
""")
print(cur.fetchall())

## 2. The Path of a Query ##

import psycopg2
conn = psycopg2.connect(dbname='hud', user='hud_admin', password='eRqg123EEkl')
cur = conn.cursor()
cur.execute("""
    EXPLAIN 
    SELECT COUNT(*) FROM homeless_by_coc 
    WHERE year > '2012-01-01';
""")
query_plan = cur.fetchall()
for row in query_plan:
    print(row[0])

## 3. Additional Output Formats ##

import psycopg2
conn = psycopg2.connect(dbname='hud', user='hud_admin', password='eRqg123EEkl')
cur = conn.cursor()
import json
cur.execute("""
    EXPLAIN (FORMAT json)
    SELECT COUNT(*) FROM homeless_by_coc 
    WHERE year > '2012-01-01';
""")
query_plan = cur.fetchone()
print(json.dumps(query_plan[0], indent=2))

## 4. Understanding Cost Estimations ##

import psycopg2
conn = psycopg2.connect(dbname='hud', user='hud_admin', password='eRqg123EEkl')
cur = conn.cursor()
cpu_tuple_cost = 0.01
cpu_operator_cost = 0.0025
seq_page_cost = 1.0
cpu_tuple_cost = 0.01
cpu_operator_cost = 0.0025
seq_page_cost = 1.0
cur.execute("""
    SELECT reltuples, relpages
    FROM pg_class
    WHERE relname = 'homeless_by_coc';
""")
n_tuple, n_page = cur.fetchone()
total_cost = (cpu_tuple_cost + cpu_operator_cost) * n_tuple + seq_page_cost * n_page
print(total_cost)

## 5. Obtaining Exact Runtimes ##

import psycopg2
import json
conn = psycopg2.connect(dbname='hud', user='hud_admin', password='eRqg123EEkl')
cur = conn.cursor()
cur.execute("""
    EXPLAIN (ANALYZE, FORMAT json)
    SELECT COUNT(*) 
    FROM homeless_by_coc 
    WHERE year > '2012-01-01';
""")
query_plan = cur.fetchone()
print(json.dumps(query_plan[0], indent=2))

## 6. Test and Rollback ##

import psycopg2
import json
conn = psycopg2.connect(dbname='hud', user='hud_admin', password='eRqg123EEkl')
cur = conn.cursor()
cur.execute("""
    EXPLAIN (ANALYZE, FORMAT json) 
    DELETE FROM state_household_incomes;
""")
conn.rollback()
query_plan = cur.fetchone()
print(json.dumps(query_plan[0], indent=2))

## 7. Analyzing a Join Statement ##

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
query_plan = cur.fetchone()
print(json.dumps(query_plan[0], indent=2))
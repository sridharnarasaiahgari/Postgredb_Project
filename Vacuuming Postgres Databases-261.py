## 1. Destructive Queries ##

import psycopg2
conn = psycopg2.connect(dbname="hud", user="hud_admin", password="eRqg123EEkl")
cur = conn.cursor()
cur.execute("SELECT * FROM homeless_by_coc;")
num_rows_before = len(cur.fetchall())
cur.execute("DELETE FROM homeless_by_coc;")
cur.execute("SELECT * FROM homeless_by_coc;")
num_rows_after = len(cur.fetchall())
print(num_rows_before)
print(num_rows_after)

## 3. Counting Dead Rows ##

import psycopg2
conn = psycopg2.connect(dbname="hud", user="hud_admin", password="eRqg123EEkl")
cur = conn.cursor()
cur.execute("""
    SELECT n_dead_tup
    FROM pg_catalog.pg_stat_all_tables 
    WHERE relname='homeless_by_coc';
""")
homeless_dead_rows = cur.fetchone()[0]
print(homeless_dead_rows)

## 4. Vacuuming Dead Rows ##

import psycopg2
conn = psycopg2.connect(dbname="hud", user="hud_admin", password="eRqg123EEkl")
cur = conn.cursor()
conn.autocommit = True
cur.execute("VACUUM VERBOSE homeless_by_coc;")
for notice in conn.notices:
    print(notice)

## 5. Transaction IDs ##

import psycopg2
conn = psycopg2.connect(dbname='hud', user='hud_admin', password='eRqg123EEkl')
cur = conn.cursor()
row = (1, '2007-01-01', 'AK', 'AK-500', 'Anchorage CoC', 'Chronically Homeless Individuals', 224)
row = (1, '2007-01-01', 'AK', 'AK-500', 'Anchorage CoC', 'Chronically Homeless Individuals', 224)
conn.autocommit = True
cur.execute("INSERT INTO homeless_by_coc VALUES (%s, %s, %s, %s, %s, %s, %s);", row)
cur.execute("VACUUM VERBOSE homeless_by_coc;")
for line in conn.notices:
    print(line)
cur.execute("SELECT xmin FROM homeless_by_coc;")
xmin = cur.fetchone()
print(xmin)

## 6. Updating Statistics ##

import json
conn = psycopg2.connect(dbname="hud", user="hud_admin", password="eRqg123EEkl")
conn.autocommit = True
cur = conn.cursor()
cur.execute("EXPLAIN SELECT * FROM homeless_by_coc;")
plan_before = cur.fetchall()

cur.execute("VACUUM ANALYZE homeless_by_coc;")

cur.execute("EXPLAIN SELECT * FROM homeless_by_coc;")
plan_after = cur.fetchall()

print(plan_before)
print(plan_after)

## 7. Full Vacuum ##

import psycopg2
conn = psycopg2.connect(dbname="hud", user="hud_admin", password="eRqg123EEkl")
conn.autocommit = True
cur = conn.cursor()
cur.execute("SELECT * FROM homeless_by_coc;")
print(cur.fetchall())

cur.execute("SELECT pg_size_pretty(pg_total_relation_size('homeless_by_coc'));")
space_before = cur.fetchone()

cur.execute("VACUUM FULL homeless_by_coc;")

cur.execute("SELECT pg_size_pretty(pg_total_relation_size('homeless_by_coc'));")
space_after = cur.fetchone()

print(space_before)
print(space_after)

## 8. Autovacuum ##

import psycopg2
conn = psycopg2.connect(dbname="hud", user="hud_admin", password="eRqg123EEkl")
conn.autocommit = True
cur = conn.cursor()
cur.execute("VACUUM homeless_by_coc;")
import time
time.sleep(1)
cur.execute("""
    SELECT last_vacuum, last_autovacuum FROM pg_stat_user_tables
    WHERE relname = 'homeless_by_coc';
""")
timestamps = cur.fetchone()
print(timestamps)
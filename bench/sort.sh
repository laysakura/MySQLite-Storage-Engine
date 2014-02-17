#!/bin/sh
db=/data/local/nakatani/sqlite3/bench_db//10000000records.sqlite3
ddl="drop table if exists large_table; create table large_table engine=mysqlite file_name='$db,large_table';"
query="select * from large_table order by val_col limit 5"
. $(cd $(dirname $0);pwd)/common.sh

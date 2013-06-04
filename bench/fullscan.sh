#!/bin/sh
db=/data/local/nakatani/sqlite3/bench_db//100000000records.sqlite3
ddl="drop table if exists large_table; create table large_table engine=mysqlite file_name='$db';"
query="select count(*) from large_table"
. $(cd $(dirname $0);pwd)/common.sh

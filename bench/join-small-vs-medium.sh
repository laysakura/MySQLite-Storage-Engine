#!/bin/sh
db=/data/local/nakatani/sqlite3/bench_db/join-small-vs-medium.sqlite
ddl="drop table if exists S; drop table if exists T; create table S engine=mysqlite file_name='$db'; create table T engine=mysqlite file_name='$db';"
query="select count(*) from T, S where T.key_col = S.key_col;"
. $(cd $(dirname $0);pwd)/common.sh

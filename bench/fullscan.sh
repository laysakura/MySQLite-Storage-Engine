#!/bin/sh
db=/data/local/nakatani/sqlite3/bench_db//100000000records.sqlite3
query="select count(*) from large_table"
. $(cd $(dirname $0);pwd)/common.sh

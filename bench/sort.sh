#!/bin/sh
db=/data/local/nakatani/sqlite3/bench_db//10000000records.sqlite3
query="select * from large_table order by val_col limit 5"
. $(cd $(dirname $0);pwd)/common.sh

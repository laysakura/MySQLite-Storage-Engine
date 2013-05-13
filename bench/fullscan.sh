#!/bin/sh
db=/data/local/nakatani/sqlite3/group_by/100000000records.sqlite3
query="select count(*) from large_table"
. $(cd $(dirname $0);pwd)/common.sh

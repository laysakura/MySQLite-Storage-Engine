#!/bin/sh
db=/data/local/nakatani/sqlite3/bench_db/join-small-vs-large.sqlite
query="select count(*) from T, S where T.key_col = S.key_col;"
. $(cd $(dirname $0);pwd)/common.sh

#!/bin/sh

db=/data/local/nakatani/sqlite3/group_by/1000000records.sqlite3

cat $db > /dev/null  # read onto page cache

cat <<EOF

-- [MySQLite] --
EOF
~/local/mysql-5.6/bin/mysql -v -v -u root test -e "select sqlite_db('$db'); select avg(val_col) from large_table group by key_col;"


cat <<EOF

-- [SQLite] --
EOF
time sqlite3 $db "select avg(val_col) from large_table group by key_col;"

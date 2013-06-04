cat $db > /dev/null  # read onto page cache

cat <<EOF
----------------------------------------
[DB]     $db
[Query]  $query
----------------------------------------
EOF

cat <<EOF

-- [MySQLite] --
EOF
~/local/mysql-5.6/bin/mysql -v -v -u root test -e "$ddl; $query;"


cat <<EOF

-- [SQLite] --
EOF
time sqlite3 $db "$query;"

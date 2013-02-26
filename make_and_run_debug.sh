#!/bin/sh
make VERBOSE=1 && (sudo make install && sudo pkill -9 mysql ; sudo -u mysql /usr/local/mysql/bin/mysqld --defaults-file=/etc/my.cnf &)

[ $? -ne 0 ] && exit 1

sleep 5

mysql -uroot mysql < support-files/uninstall.sql
mysql -uroot mysql < support-files/install.sql

echo "Current TODOs:"
grep -rin 'TODO' src/* t/*.t

#!/bin/sh

. node_conf.sh

(rm -rf *.cmake CMakeFiles CMakeCache.txt ; cd ../.. ; cmake -DCMAKE_BUILD_TYPE=Release .)
make VERBOSE=1 && (make install && pkill mysql ; $basedir/bin/mysqld --defaults-file=/home/nakatani/local/etc/my.cnf-5.6 &)

[ $? -ne 0 ] && exit 1

sleep 5

$basedir/bin/mysql -uroot mysql < support-files/uninstall.sql
$basedir/bin/mysql -uroot mysql < support-files/install.sql

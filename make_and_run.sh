#!/bin/sh

. ./node_conf.sh

(rm -rf *.cmake CMakeFiles CMakeCache.txt ; cd ../.. ; cmake -DCMAKE_BUILD_TYPE=Release .)
make VERBOSE=1 && ($sudo make install && $sudo pkill mysql ; $sudo_mysql $basedir/bin/mysqld --defaults-file=$my_cnf &)

[ $? -ne 0 ] && exit 1

. ./common-commands.sh

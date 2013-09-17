#!/bin/sh

. ./node_conf.sh

(rm -rf *.cmake CMakeFiles CMakeCache.txt ; cd ../.. ; cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-DDBUG_OFF" .)
make VERBOSE=1 && ($sudo make install && $sudo pkill mysql ; $sudo_mysql $basedir/bin/mysqld --defaults-file=$my_cnf &)

[ $? -ne 0 ] && exit 1

/bin/sh ./common-commands.sh

$sudo gdb -p $(pgrep mysqld)

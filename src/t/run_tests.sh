#!/bin/sh

run_test() {
    t=$1
    t_name=$(basename $t)
    if $t > /dev/null 2>&1; then
        if valgrind --tool=memcheck --leak-check=full $t > /dev/null 2>&1; then
            echo "[$t_name] ok"
        else
            echo "[$t_name] memory leak!!"
        fi
    else
        echo "[$t_name] fails!!"
    fi
}

basedir=$(cd $(dirname $0); pwd)
for t in $(find $basedir -maxdepth 1 -executable -type f |grep -v $0); do run_test $t; done

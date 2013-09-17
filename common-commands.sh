#!/bin/sh

. ./node_conf.sh

sleep 3
$basedir/bin/mysql -uroot mysql < support-files/uninstall.sql
$basedir/bin/mysql -uroot mysql < support-files/install.sql
$basedir/bin/mysql -uroot mysql -e "create table not_used_name engine=mysqlite file_name='$enginedir/t/db/08-ALWAYS-EXISTS.sqlite'"
# $basedir/bin/mysql -uroot mysql -e "create table not_used_name engine=mysqlite file_name='$enginedir/t/db/08-ALWAYS-EXISTS2.sqlite'"

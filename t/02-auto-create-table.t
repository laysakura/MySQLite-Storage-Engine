#! /usr/bin/perl

use strict;
use warnings;

use DBI;

use Test::More tests => 15;

use File::Basename;
use Cwd 'realpath';
my $testdir = realpath(dirname(__FILE__));

my $db = $ENV{DB} || "test";
my $cnf = $ENV{CNF} || "/etc/my.cnf";
my $dbh = DBI->connect(
    $ENV{DBI} || "dbi:mysql:$db;mysql_read_default_file=$cnf",
    $ENV{DBI_USER} || 'root',
    $ENV{DBI_PASSWORD} || '',
) or die 'connection failed:';

# 1 table with short DDL
ok($dbh->do("drop table if exists test02_table1_ddl_short_t1"));
ok($dbh->do("create table test02_table1_ddl_short_t1 engine=mysqlite file_name='$testdir/db/02-table1-ddl_short.sqlite'"));
is_deeply(
    $dbh->selectall_arrayref("select column_name from information_schema.columns where table_schema='$db' and table_name='test02_table1_ddl_short_t1'"),
    [ ['c1'] ],
);


# 3 tables w/ short schema
ok($dbh->do("drop table if exists test02_table3_ddl_short_t1"));
ok($dbh->do("drop table if exists test02_table3_ddl_short_t2"));
ok($dbh->do("drop table if exists test02_table3_ddl_short_t3"));
ok($dbh->do("create table test02_table3_ddl_short_t1 engine=mysqlite file_name='$testdir/db/02-table3-ddl_short.sqlite'"));
ok($dbh->do("create table test02_table3_ddl_short_t2 engine=mysqlite file_name='$testdir/db/02-table3-ddl_short.sqlite'"));
ok($dbh->do("create table test02_table3_ddl_short_t3 engine=mysqlite file_name='$testdir/db/02-table3-ddl_short.sqlite'"));
is_deeply(
    $dbh->selectall_arrayref("select column_name from information_schema.columns where table_schema='$db' and table_name='test02_table3_ddl_short_t1'"),
    [ ['c1'] ],
);
is_deeply(
    $dbh->selectall_arrayref("select column_name from information_schema.columns where table_schema='$db' and table_name='test02_table3_ddl_short_t2'"),
    [ ['cc1'] ],
);
is_deeply(
    $dbh->selectall_arrayref("select column_name from information_schema.columns where table_schema='$db' and table_name='test02_table3_ddl_short_t3'"),
    [ ['ccc1'] ],
);


# 1 table w/ so long schema (that it exeeds page#1 of SQLite DB)
ok($dbh->do("drop table if exists test02_table1_ddl_long_t20"));
ok($dbh->do("create table test02_table1_ddl_long_t20 engine=mysqlite file_name='$testdir/db/02-table1-ddl_long.sqlite'"));
is_deeply(
    $dbh->selectall_arrayref("select column_name from information_schema.columns where table_schema='$db' and table_name='test02_table1_ddl_long_t20'"),
    [ ['col20'] ],
);

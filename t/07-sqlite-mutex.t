#! /usr/bin/perl

use strict;
use warnings;

use DBI;

use Test::More tests => 7;
use Test::Deep;

use File::Basename;
use Cwd 'realpath';
my $testdir = realpath(dirname(__FILE__));

my $db = $ENV{DB} || "test";
my $cnf = $ENV{CNF} || "/etc/my.cnf";
my $dbh_mysql = DBI->connect(
    $ENV{DBI} || "dbi:mysql:$db;mysql_read_default_file=$cnf",
    $ENV{DBI_USER} || 'root',
    $ENV{DBI_PASSWORD} || '',
    {AutoInactiveDestroy => 1},
) or die 'connection failed:';

my $dbpath = "$testdir/db/07-a.sqlite";
unlink $dbpath;

my $dbh_sqlite = DBI->connect("dbi:SQLite:dbname=$dbpath", '', '');

# create initial table
ok($dbh_sqlite->do("create table T (a INT)"));
ok($dbh_sqlite->do("insert into T values (1)"));

# mysql connects to the DB file
ok($dbh_mysql->do("drop table if exists T"));
ok($dbh_mysql->do("create table T engine=mysqlite file_name='$dbpath'"));


# (1) mysql takes R lock [child],
# (2) sqlite W lock waits until (1) finishes [parent]
unless (fork) {
    $dbh_mysql->do("lock tables T read") or die "failed to lock table";

    eq_deeply($dbh_sqlite->selectall_arrayref("select * from T"), [ [1] ]) or die "wiered table contents";

    sleep 2;

    ## since W lock waits until "unlock tables" below,
    ## the contents of sqlite db doesn't change between sleep.
    eq_deeply($dbh_sqlite->selectall_arrayref("select * from T"), [ [1] ]) or die "table contents has been changed while taking read lock";

    $dbh_mysql->do("unlock tables") or die "failed to unlock table";
    exit;
}
ok($dbh_sqlite->do("insert into T values (2)"));  # returns after "unlock tables" finished
is_deeply(
    $dbh_sqlite->selectall_arrayref("select * from T"),
    [ [1], [2] ],
);
# check if mysqlite see the FileChangeCounter of SQLite DB
# and refresh page cache on SQLite DB update.
is_deeply(
    $dbh_mysql->selectall_arrayref("select * from T"),
    [ [1], [2] ],
);

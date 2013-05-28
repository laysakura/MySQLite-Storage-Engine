#! /usr/bin/perl

use strict;
use warnings;

use DBI;

use Test::More tests => 8;

use File::Basename;
use Cwd 'realpath';
my $testdir = realpath(dirname(__FILE__));

my $dbh = DBI->connect(
    $ENV{DBI} || "dbi:mysql:test;mysql_read_default_file=$ENV{HOME}/.my.cnf",
    $ENV{DBI_USER} || 'root',
    $ENV{DBI_PASSWORD} || '',
) or die 'connection failed:';

# Reading existing DB
ok($dbh->do("drop table if exists T0"));
ok($dbh->do("create table T0 engine=mysqlite file_name='$testdir/db/01-a.sqlite'"));


# Checking if new DB is really created
SKIP: {
    skip 'Writing to SQLite DB is not supported yet', 2 if 1;
    is_deeply(
        $dbh->selectall_arrayref("select sqlite_db('$testdir/db/01-NEW.sqlite')"),
        [ [0] ],
    );
    ok(-e "$testdir/db/01-NEW.sqlite");
    unlink("$testdir/db/01-NEW.sqlite");
}


# Error on creating non-existing table
ok($dbh->do("drop table if exists T0"));
ok(! $dbh->do("create table T100 engine=mysqlite file_name='$testdir/db/01-a.sqlite'"));


# Error on reading illegal SQLite3 DB
ok($dbh->do("drop table if exists T0"));
ok(! $dbh->do("create table T0 engine=mysqlite file_name='$testdir/db/01-illegal.sqlite'"));

#! /usr/bin/perl

use strict;
use warnings;

use DBI;

my $TEST_ROWS;
BEGIN {
    $TEST_ROWS = $ENV{TEST_ROWS} || 1024;
};

use Test::More tests => $TEST_ROWS * 4 + 2;

use File::Basename;
use Cwd 'realpath';
my $testdir = realpath(dirname(__FILE__));

my $dbh = DBI->connect(
    $ENV{DBI} || 'dbi:mysql:database=test;host=localhost',
    $ENV{DBI_USER} || 'root',
    $ENV{DBI_PASSWORD} || '',
) or die 'connection failed:';


## Crash bug repro
ok($dbh->do("drop table if exists t_example"));
ok($dbh->do("create table t_example (a INT) engine = mysqlite"));

for (my $i = 0; $i < $TEST_ROWS; $i++) {
    ok($dbh->do("insert into t_example values (777),(333),(888)"));
}
for (my $i = 0; $i < $TEST_ROWS; $i++) {
    ok($dbh->do("select * from t_example"));
}
for (my $i = 0; $i < $TEST_ROWS; $i++) {
    ok($dbh->do("insert into t_example values (777),(333),(888)"));
    ok($dbh->do("select * from t_example"));
}

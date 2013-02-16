#! /usr/bin/perl

use strict;
use warnings;

use DBI;

use Test::More tests => 3;

use File::Basename;
use Cwd 'realpath';
my $testdir = realpath(dirname(__FILE__));

my $dbh = DBI->connect(
    $ENV{DBI} || 'dbi:mysql:database=test;host=localhost',
    $ENV{DBI_USER} || 'root',
    $ENV{DBI_PASSWORD} || '',
) or die 'connection failed:';


## Simple full scan
ok($dbh->do("drop table if exists test02_table1_ddl_short_t1"));
ok($dbh->do("select sqlite_db('$testdir/db/03-simple-beer.sqlite')"));
is_deeply(
    $dbh->selectall_arrayref("select * from japan"),
    [
        ['Kuro-Label', 'Sapporo'],
        ['Ichiban Shibori', 'Kirin'],
        ['Super Dry', 'Asahi'],
    ],
);


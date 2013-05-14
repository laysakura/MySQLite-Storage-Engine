#! /usr/bin/perl

use strict;
use warnings;

use DBI;

use Test::More tests => 5;

use File::Basename;
use Cwd 'realpath';
my $testdir = realpath(dirname(__FILE__));

my $dbh = DBI->connect(
    $ENV{DBI} || "dbi:mysql:test;mysql_read_default_file=$ENV{HOME}/.my.cnf",
    $ENV{DBI_USER} || 'root',
    $ENV{DBI_PASSWORD} || '',
) or die 'connection failed:';


## Small-vs-large join
ok($dbh->do("drop table if exists T, S"));
ok($dbh->do("select sqlite_db('$testdir/db/join-small-vs-large.sqlite')"));
is_deeply(
    $dbh->selectall_arrayref("select count(*) from T, S where T.key_col = S.key_col;"),
    [ [1000] ],
);

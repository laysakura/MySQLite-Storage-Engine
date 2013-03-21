#! /usr/bin/perl

use strict;
use warnings;

use DBI;

use Test::More tests => 1;

use File::Basename;
use Cwd 'realpath';
my $testdir = realpath(dirname(__FILE__));

my $dbh = DBI->connect(
    $ENV{DBI} || 'dbi:mysql:database=test;host=localhost',
    $ENV{DBI_USER} || 'root',
    $ENV{DBI_PASSWORD} || '',
) or die 'connection failed:';

## Apply sqlite_db() to a SQLite db file which include
## DDL mysqld cannnot understand
TODO: {
    local $TODO = 'DDL of SQLite incompatible with MySQL is not supported yet.';
    ok 0;
};

#! /usr/bin/perl

use strict;
use warnings;

use DBI;

use Test::More tests => 5;

use File::Basename;
use Cwd 'realpath';
my $testdir = realpath(dirname(__FILE__));

my $dbh = DBI->connect(
    $ENV{DBI} || 'dbi:mysql:database=test;host=localhost',
    $ENV{DBI_USER} || 'root',
    $ENV{DBI_PASSWORD} || '',
) or die 'connection failed:';

##
is_deeply(
    $dbh->selectall_arrayref("select sqlite_db('$testdir/db/01-open-db-a.sqlite')"),
    [ [1] ],
    'Test: Reading existing DB');

##
is_deeply(
    $dbh->selectall_arrayref("select sqlite_db('$testdir/db/01-open-db-NEW.sqlite')"),
    [ [0] ],
    'Test: Reading existing DB');
ok(-e "$testdir/db/01-open-db-NEW.sqlite",
   'Test: Checking if new DB is really created');
unlink("$testdir/db/01-open-db-NEW.sqlite");

##
is($dbh->do("select sqlite_db('$testdir/db/01-open-db-illegal.sqlite')"),
   undef,
   'Test: Error on reading illegal SQLite3 DB',
);

##
is($dbh->do("select sqlite_db('/path/to/non/existing/dir/foo.sqlite')"),
   undef,
   'Test: Error on accessing non-existing directory',
);

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

# Reading existing DB
is_deeply(
    $dbh->selectall_arrayref("select sqlite_db('$testdir/db/01-a.sqlite')"),
    [ [1] ],
);

# Checking if new DB is really created
is_deeply(
    $dbh->selectall_arrayref("select sqlite_db('$testdir/db/01-NEW.sqlite')"),
    [ [0] ],
);
ok(-e "$testdir/db/01-NEW.sqlite");
unlink("$testdir/db/01-NEW.sqlite");

# Error on reading illegal SQLite3 DB
is(
    $dbh->do("select sqlite_db('$testdir/db/01-illegal.sqlite')"),
    undef,
);

# Error on accessing non-existing directory
is(
    $dbh->do("select sqlite_db('/path/to/non/existing/dir/foo.sqlite')"),
    undef,
);

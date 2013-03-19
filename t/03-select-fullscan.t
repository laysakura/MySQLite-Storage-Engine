#! /usr/bin/perl

use strict;
use warnings;

use DBI;

use Test::More tests => 8;

use File::Basename;
use Cwd 'realpath';
my $testdir = realpath(dirname(__FILE__));

my $dbh = DBI->connect(
    $ENV{DBI} || 'dbi:mysql:database=test;host=localhost',
    $ENV{DBI_USER} || 'root',
    $ENV{DBI_PASSWORD} || '',
) or die 'connection failed:';


## Simple full scan
ok($dbh->do("drop table if exists japan"));
ok($dbh->do("select sqlite_db('$testdir/db/03-simple-beer.sqlite')"));
is_deeply(
    $dbh->selectall_arrayref("select * from japan"),
    [
        ['Kuro-Label', 'Sapporo'],
        ['Ichiban Shibori', 'Kirin'],
        ['Super Dry', 'Asahi'],
    ],
);
is_deeply(
    $dbh->selectall_arrayref("select name from japan"),
    [
        ['Kuro-Label'],
        ['Ichiban Shibori'],
        ['Super Dry'],
    ],
);
is_deeply(
    $dbh->selectall_arrayref("select maker from japan"),
    [
        ['Sapporo'],
        ['Kirin'],
        ['Asahi'],
    ],
);

## Japanese Support
TODO: {
    local $TODO = 'Japanese support';
    ok($dbh->do("drop table if exists japan"));
    ok($dbh->do("select sqlite_db('$testdir/db/03-simple-beer-jp.sqlite')"));
    is_deeply(
        $dbh->selectall_arrayref("select * from japan"),
        [
            ['黒ラベル', 'サッポロ'],
            ['一番絞り', 'キリン'],
            ['スーパードライ', 'アサヒ'],
        ],
    );
}

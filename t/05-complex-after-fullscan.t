#! /usr/bin/perl

use strict;
use warnings;

use DBI;

use Test::More tests => 7;

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


ok($dbh->do("drop table if exists Beer"));
ok($dbh->do("drop table if exists Company"));
ok($dbh->do("create table Beer engine=mysqlite file_name='$testdir/db/BeerDB-small.sqlite'"));
ok($dbh->do("create table Company engine=mysqlite file_name='$testdir/db/BeerDB-small.sqlite'"));

## Group by
is_deeply(
    $dbh->selectall_arrayref("select maker, avg(price) from Beer group by maker"),
    [
        ['Anchor', '525.0000'],
        ['Sankt Gallen', '450.0000'],
        ['Sapporo', '300.0000'],
    ],
);

## Sort
is_deeply(
    $dbh->selectall_arrayref("select maker, name from Beer order by maker"),
    [
        ['Anchor', 'Porter'],
        ['Anchor', 'Liberty Ale'],
        ['Sankt Gallen', 'Shonan Gold'],
        ['Sankt Gallen', 'Sakura'],
        ['Sankt Gallen', 'Golden Ale'],
        ['Sapporo', 'Ebisu'],
        ['Sapporo', 'Kuro Label'],
    ],
);

## Join
is_deeply(
    $dbh->selectall_arrayref("select Beer.maker, Beer.name, Company.country from Beer, Company where Beer.maker = Company.maker;"),
    [
        ['Anchor', 'Porter', 'America'],
        ['Anchor', 'Liberty Ale', 'America'],
        ['Sankt Gallen', 'Shonan Gold', 'Japan'],
        ['Sankt Gallen', 'Sakura', 'Japan'],
        ['Sankt Gallen', 'Golden Ale', 'Japan'],
        ['Sapporo', 'Ebisu', 'Japan'],
        ['Sapporo', 'Kuro Label', 'Japan'],
    ],
);

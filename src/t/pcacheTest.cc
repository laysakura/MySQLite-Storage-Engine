#include <gtest/gtest.h>

#include "../pcache.h"
#include "../sqlite_format.h"
#include "../mysqlite_config.h"


TEST(pcache, correct_DBHeader)
{
  errstat res;
  PageCache *pcache = PageCache::get_instance();
  pcache->alloc(1024 * 100);

  FILE *f = fopen(MYSQLITE_TEST_DB_DIR "/TableLeafPage-2tables.sqlite", "r");
  res = pcache->refresh(f);
  ASSERT_EQ(res, MYSQLITE_OK);

  ASSERT_STREQ(SQLITE3_SIGNATURE, (char *)pcache->fetch(1));
  ASSERT_EQ(DbHeader::get_pg_sz(), 1024);

  pcache->free();
}

TEST(pcache, SmallerPageCacheThanDbFile)
{
  errstat res;
  PageCache *pcache = PageCache::get_instance();
  pcache->alloc(PCACHE_MIN_SZ);

  FILE *f = fopen(MYSQLITE_TEST_DB_DIR "/wikipedia.sqlite", "r");
  res = pcache->refresh(f);
  ASSERT_EQ(res, MYSQLITE_OK);

  ASSERT_STREQ(SQLITE3_SIGNATURE, (char *)pcache->fetch(1));

  pcache->free();
}

TEST(pcache, refresh)
{
  errstat res;
  PageCache *pcache = PageCache::get_instance();
  pcache->alloc(1024 * 100);

  u8 first_page2[1024], second_page2[1024];

  FILE *f = fopen(MYSQLITE_TEST_DB_DIR "/TableLeafPage-2tables.sqlite", "r");

  { // 1st fetch
    res = pcache->refresh(f);
    ASSERT_EQ(res, MYSQLITE_OK);
    memcpy(first_page2, pcache->fetch(2), 1024);
  }
  { // 2nd fetch
    res = pcache->refresh(f);
    ASSERT_EQ(res, MYSQLITE_OK);
    memcpy(second_page2, pcache->fetch(2), 1024);
  }

  ASSERT_EQ(memcmp(first_page2, second_page2, 1024), 0);

  pcache->free();
}

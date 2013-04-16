#include <gtest/gtest.h>

#include "../pcache.h"
#include "../sqlite_format.h"
#include "../mysqlite_config.h"


TEST(pcache, correct_DBHeader)
{
  errstat res;
  PageCache *pcache = PageCache::get_instance();
  pcache->alloc(1024 * 100);

  res = pcache->refresh(MYSQLITE_TEST_DB_DIR "/TableLeafPage-2tables.sqlite");
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

  res = pcache->refresh(MYSQLITE_TEST_DB_DIR "/wikipedia.sqlite");
  ASSERT_EQ(res, MYSQLITE_OK);

  ASSERT_STREQ(SQLITE3_SIGNATURE, (char *)pcache->fetch(1));

  pcache->free();
}

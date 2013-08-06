#include <gtest/gtest.h>

#include "../pcache_mmap.h"
#include "../sqlite_format.h"
#include "../mysqlite_config.h"


TEST(pcache, correct_DBHeader)
{
  errstat res;
  PageCache *pcache = PageCache::get_instance();

  res = pcache->open(MYSQLITE_TEST_DB_DIR "/TableLeafPage-2tables.sqlite");
  ASSERT_EQ(res, MYSQLITE_OK);

  pcache->rd_lock();

  ASSERT_STREQ(SQLITE3_SIGNATURE, (char *)pcache->fetch(1));
  ASSERT_EQ(DbHeader::get_pg_sz(), 1024);

  pcache->unlock();

  pcache->close();
}

TEST(pcache, lock)
{
  errstat res;
  PageCache *pcache = PageCache::get_instance();

  res = pcache->open(MYSQLITE_TEST_DB_DIR "/TableLeafPage-2tables.sqlite");
  ASSERT_EQ(res, MYSQLITE_OK);

  pcache->rd_lock();
  u16 fcc = DbHeader::get_file_change_counter();
  pcache->unlock();

  pcache->rd_lock();
  u16 fcc2 = DbHeader::get_file_change_counter();
  ASSERT_GE(fcc2, fcc);

  printf("fcc=%d, fcc2=%d\n", fcc, fcc2);

  ASSERT_EQ(MYSQLITE_FLOCK_NEEDED, DbHeader::inc_file_change_counter()); // write lock is necessary

  pcache->upgrade_lock();
  ASSERT_EQ(MYSQLITE_OK, DbHeader::inc_file_change_counter());
  pcache->unlock();

  pcache->close();
}


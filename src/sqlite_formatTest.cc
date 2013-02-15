#include <gtest/gtest.h>

#include "sqlite_format.h"

TEST(BtreePage, BtreeHeaderAnalysis)
{
  FILE *f_db = open_sqlite_db("/home/nakatani/git/mysql-sqlite-engine/src/t/db/BtreeHeaderAnalysis-simple.sqlite");
  DbHeader db_header(f_db);
  ASSERT_TRUE(db_header.read());

  TableLeafPage tbl_leaf(f_db, &db_header, 2);
  ASSERT_TRUE(tbl_leaf.read());

  EXPECT_EQ(tbl_leaf.get_btree_type(), TABLE_LEAF);

  fclose(f_db);
}

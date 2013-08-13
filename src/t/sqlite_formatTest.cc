#include <gtest/gtest.h>
#include <bits/unique_ptr.h>

#include "../mysqlite_api.h"
#include "../sqlite_format.h"
#include "../mysqlite_config.h"


/*
** BtreePage
*/
class TBtreePage : public BtreePage {
public:
  TBtreePage(Pgno pg_id)
    : BtreePage(pg_id)
  {}
  Pgsz get_ith_cell_offset(Pgsz i) {
    return BtreePage::get_ith_cell_offset(i);
  }
  bool is_valid_hdr() const {
    return BtreePage::is_valid_hdr();
  }
};

TEST(SqliteDb, usage)
{
  unlink(MYSQLITE_TEST_DB_DIR "/not-exist.sqlite");
  {
    std::unique_ptr<SqliteDb> sqlite_db;
    sqlite_db.reset(new SqliteDb("/invalid/path/a.db"));
    ASSERT_EQ(sqlite_db->mode(), SqliteDb::FAIL);
  }
  {
    std::unique_ptr<SqliteDb> sqlite_db;
    sqlite_db.reset(new SqliteDb(MYSQLITE_TEST_DB_DIR "/01-illegal.sqlite"));
    ASSERT_EQ(sqlite_db->mode(), SqliteDb::FAIL);
  }
  {
    struct stat st;
    ASSERT_EQ(stat(MYSQLITE_TEST_DB_DIR "/not-exist.sqlite", &st), -1);

    std::unique_ptr<SqliteDb> sqlite_db;
    sqlite_db.reset(new SqliteDb(MYSQLITE_TEST_DB_DIR "/not-exist.sqlite", true));
    ASSERT_EQ(sqlite_db->mode(), SqliteDb::FAIL);

    ASSERT_EQ(stat(MYSQLITE_TEST_DB_DIR "/not-exist.sqlite", &st), -1);
  }
  {
    std::unique_ptr<SqliteDb> sqlite_db;
    sqlite_db.reset(new SqliteDb(MYSQLITE_TEST_DB_DIR "/BeerDB-small.sqlite"));
    ASSERT_EQ(sqlite_db->mode(), SqliteDb::READ_WRITE);
  }
  {
    struct stat st;
    ASSERT_EQ(stat(MYSQLITE_TEST_DB_DIR "/not-exist.sqlite", &st), -1);  // not exists

    std::unique_ptr<SqliteDb> sqlite_db;
    sqlite_db.reset(new SqliteDb(MYSQLITE_TEST_DB_DIR "/not-exist.sqlite"));
    ASSERT_EQ(sqlite_db->mode(), SqliteDb::READ_WRITE);

    ASSERT_EQ(stat(MYSQLITE_TEST_DB_DIR "/not-exist.sqlite", &st), 0);   // created
    ASSERT_EQ(unlink(MYSQLITE_TEST_DB_DIR "/not-exist.sqlite"), 0);
  }
  {
    struct stat st;
    ASSERT_EQ(stat(MYSQLITE_TEST_DB_DIR "/not-exist.sqlite", &st), -1);  // not exists
    int fd = open(MYSQLITE_TEST_DB_DIR "/not-exist.sqlite", O_RDWR | O_CREAT); // create empty file

    std::unique_ptr<SqliteDb> sqlite_db;
    sqlite_db.reset(new SqliteDb(MYSQLITE_TEST_DB_DIR "/not-exist.sqlite"));
    ASSERT_EQ(sqlite_db->mode(), SqliteDb::READ_WRITE);

    close(fd);
    ASSERT_EQ(unlink(MYSQLITE_TEST_DB_DIR "/not-exist.sqlite"), 0);
  }
  {
    std::unique_ptr<SqliteDb> sqlite_db;
    sqlite_db.reset(new SqliteDb(MYSQLITE_TEST_DB_DIR "/BeerDB-small.sqlite", true));
    ASSERT_EQ(sqlite_db->mode(), SqliteDb::READ_ONLY);
  }


}

TEST(BtreePage, BtreePageValidityCheck_success)
{
  using namespace mysqlite;

  Connection conn;
  errstat res = conn.open(MYSQLITE_TEST_DB_DIR "/BeerDB-small.sqlite");
  ASSERT_EQ(res, MYSQLITE_OK);

  TBtreePage btree_page(2);
  conn.rdlock_db();
  ASSERT_EQ(MYSQLITE_OK, btree_page.fetch());
  ASSERT_TRUE(btree_page.is_valid_hdr());
  conn.unlock_db();

  conn.close();
}
TEST(BtreePage, BtreePageValidityCheck_page1)
{
  using namespace mysqlite;

  Connection conn;
  errstat res = conn.open(MYSQLITE_TEST_DB_DIR "/BtreePage-empty-table.sqlite");
  ASSERT_EQ(res, MYSQLITE_OK);

  TBtreePage btree_page(1);
  conn.rdlock_db();
  ASSERT_EQ(MYSQLITE_OK, btree_page.fetch());
  ASSERT_TRUE(btree_page.is_valid_hdr());
  conn.unlock_db();

  conn.close();
}

TEST(BtreePage, get_ith_cell_offset_EmptyTable)
{
  using namespace mysqlite;

  Connection conn;
  errstat res = conn.open(MYSQLITE_TEST_DB_DIR "/BtreePage-empty-table.sqlite");
  ASSERT_EQ(res, MYSQLITE_OK);

  TBtreePage btree_page(2);
  conn.rdlock_db();
  ASSERT_EQ(MYSQLITE_OK, btree_page.fetch());
  ASSERT_EQ(0, btree_page.get_ith_cell_offset(0));
  conn.unlock_db();

  conn.close();
}
TEST(BtreePage, get_ith_cell_offset_2CellsTable)
{
  using namespace mysqlite;

  Connection conn;
  errstat res = conn.open(MYSQLITE_TEST_DB_DIR "/BtreePage-2cells-table.sqlite");
  ASSERT_EQ(res, MYSQLITE_OK);

  TBtreePage btree_page(2);
  conn.rdlock_db();
  ASSERT_EQ(MYSQLITE_OK, btree_page.fetch());
  {
    ASSERT_GT(btree_page.get_ith_cell_offset(0), 0);
    ASSERT_LT(btree_page.get_ith_cell_offset(0), DbHeader::get_pg_sz());
  }
  {
    ASSERT_GT(btree_page.get_ith_cell_offset(1), 0);
    ASSERT_LT(btree_page.get_ith_cell_offset(1), DbHeader::get_pg_sz());
  }
  {
    ASSERT_EQ(btree_page.get_ith_cell_offset(2), 0);
  }
  conn.unlock_db();

  conn.close();
}


/*
** TableLeafPage
*/
TEST(TableLeafPage, get_ith_cell_2CellsTable)
{
  using namespace mysqlite;

  Connection conn;
  errstat res = conn.open(MYSQLITE_TEST_DB_DIR "/TableLeafPage-int.sqlite");
  ASSERT_EQ(res, MYSQLITE_OK);

  TableLeafPage tbl_leaf_page(2);
  conn.rdlock_db();
  ASSERT_EQ(MYSQLITE_OK, tbl_leaf_page.fetch());
  {
    for (u64 row = 0; row < 2; ++row) {
      RecordCell cell;

      ASSERT_TRUE(tbl_leaf_page.get_ith_cell(row, &cell));
      ASSERT_EQ(cell.rowid, row + 1);
      ASSERT_EQ(cell.overflow_pgno, 0u);

      for (u64 col = 0; col < 2; ++col) {
        if (row == 0 && col == 0) {
          // ST_C1 is used for value `1' instead of ST_INT8,
          // which has 0 length and can be specified only by stype.
          ASSERT_EQ(cell.payload.cols_type[col], ST_C1);
          ASSERT_EQ(cell.payload.cols_len[col], 0u);
        }
        else {
          ASSERT_EQ(cell.payload.cols_type[col], ST_INT8);
          ASSERT_EQ(2*row + col + 1,
                    u8s_to_val<u8>(
                      &cell.payload.data[cell.payload.cols_offset[col]],
                      cell.payload.cols_len[col]
                    ));
        }
      }
    }
  }
  conn.unlock_db();

  conn.close();
}
TEST(TableLeafPage, get_ith_cell_GetTableSchema)
{
  using namespace mysqlite;

  Connection conn;
  errstat res = conn.open(MYSQLITE_TEST_DB_DIR "/TableLeafPage-2tables.sqlite");
  ASSERT_EQ(res, MYSQLITE_OK);

  TableLeafPage tbl_leaf_page(1); // sqlite_master
  conn.rdlock_db();
  ASSERT_EQ(MYSQLITE_OK, tbl_leaf_page.fetch());
  {
    for (u64 row = 0; row < 2; ++row) {
      RecordCell cell;

      ASSERT_TRUE(tbl_leaf_page.get_ith_cell(row, &cell));
      ASSERT_EQ(cell.rowid, row + 1);
      ASSERT_EQ(cell.overflow_pgno, 0u);

      ASSERT_EQ(cell.payload.cols_type[SQLITE_MASTER_COLNO_SQL], ST_TEXT);
      string data((char *)&cell.payload.data[cell.payload.cols_offset[SQLITE_MASTER_COLNO_SQL]],
                  cell.payload.cols_len[SQLITE_MASTER_COLNO_SQL]);
      char answer[100];
      sprintf(answer, "CREATE TABLE t%llu (c1 INT, c2 INT)", cell.rowid);
      ASSERT_STREQ(data.c_str(), answer);
    }
  }
  conn.unlock_db();

  conn.close();
}
TEST(TableLeafPage, get_ith_cell_OverflowPage)
{
  using namespace mysqlite;

  Connection conn;
  errstat res = conn.open(MYSQLITE_TEST_DB_DIR "/TableLeafPage-overflowpage.sqlite");
  ASSERT_EQ(res, MYSQLITE_OK);

  TableLeafPage tbl_leaf_page(2);
  conn.rdlock_db();
  ASSERT_EQ(MYSQLITE_OK, tbl_leaf_page.fetch());
  {
    RecordCell cell;

    ASSERT_FALSE(tbl_leaf_page.get_ith_cell(0, &cell));
    ASSERT_EQ(cell.rowid, 1u);
    ASSERT_NE(cell.overflow_pgno, 0u);  // This suggests there are overflow pages
    ASSERT_GT(cell.payload_sz_in_origpg, 0u);
    ASSERT_LT(cell.payload_sz_in_origpg, cell.payload_sz);

    u8 *payload_data = new u8[cell.payload_sz];
    ASSERT_TRUE(tbl_leaf_page.get_ith_cell(0, &cell, payload_data));
    ASSERT_EQ(cell.payload.cols_type[0], ST_TEXT);
    string data((char *)&cell.payload.data[cell.payload.cols_offset[0]],
                cell.payload.cols_len[0]);
    string answer(1000, 'a');
    ASSERT_STREQ(data.c_str(), answer.c_str());
    delete payload_data;
  }
  conn.unlock_db();

  conn.close();
}
TEST(TableLeafPage, get_ith_cell_OverflowPage10000)
{
  using namespace mysqlite;

  Connection conn;
  errstat res = conn.open(MYSQLITE_TEST_DB_DIR "/TableLeafPage-overflowpage10000.sqlite");
  ASSERT_EQ(res, MYSQLITE_OK);

  TableLeafPage tbl_leaf_page(2);
  conn.rdlock_db();
  ASSERT_EQ(MYSQLITE_OK, tbl_leaf_page.fetch());
  {
    RecordCell cell;

    ASSERT_FALSE(tbl_leaf_page.get_ith_cell(0, &cell));
    ASSERT_EQ(cell.rowid, 1u);
    ASSERT_NE(cell.overflow_pgno, 0u);
    ASSERT_GT(cell.payload_sz_in_origpg, 0u);
    ASSERT_LT(cell.payload_sz_in_origpg, cell.payload_sz);

    u8 *payload_data = new u8[cell.payload_sz];
    ASSERT_TRUE(tbl_leaf_page.get_ith_cell(0, &cell, payload_data));
    ASSERT_EQ(cell.payload.cols_type[0], ST_TEXT);
    string data((char *)&cell.payload.data[cell.payload.cols_offset[0]],
                cell.payload.cols_len[0]);
    string answer(10000, 'a');
    ASSERT_STREQ(data.c_str(), answer.c_str());
    delete payload_data;
  }
  conn.unlock_db();

  conn.close();
}

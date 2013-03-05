#include <gtest/gtest.h>

#include "sqlite_format.h"


/*
** BtreePage
*/
class TBtreePage : public BtreePage {
public:
  TBtreePage(FILE * const f_db,
             const DbHeader * const db_header,
             Pgno pg_id)
    : BtreePage(f_db, db_header, pg_id)
  {}
  Pgsz get_ith_cell_offset(Pgsz i) {
    return BtreePage::get_ith_cell_offset(i);
  }
  bool is_valid_hdr() const {
    return BtreePage::is_valid_hdr();
  }
};

TEST(BtreePage, BtreePageValidityCheck_success)
{
  FILE *f_db = open_sqlite_db("t/db/BtreePage-empty-table.sqlite");
  ASSERT_TRUE(f_db);

  DbHeader db_header(f_db);
  ASSERT_TRUE(db_header.read());

  TBtreePage btree_page(f_db, &db_header, 2);
  ASSERT_TRUE(btree_page.read());
  ASSERT_TRUE(btree_page.is_valid_hdr());

  fclose(f_db);
}
TEST(BtreePage, BtreePageValidityCheck_page1)
{
  FILE *f_db = open_sqlite_db("t/db/BtreePage-empty-table.sqlite");
  ASSERT_TRUE(f_db);

  DbHeader db_header(f_db);
  ASSERT_TRUE(db_header.read());

  TBtreePage btree_page(f_db, &db_header, 1);
  ASSERT_TRUE(btree_page.read());
  ASSERT_TRUE(btree_page.is_valid_hdr());

  fclose(f_db);
}

TEST(BtreePage, get_ith_cell_offset_EmptyTable)
{
  FILE *f_db = open_sqlite_db("t/db/BtreePage-empty-table.sqlite");
  ASSERT_TRUE(f_db);

  DbHeader db_header(f_db);
  ASSERT_TRUE(db_header.read());

  TBtreePage btree_page(f_db, &db_header, 2);
  ASSERT_TRUE(btree_page.read());

  ASSERT_EQ(0, btree_page.get_ith_cell_offset(0));

  fclose(f_db);
}
TEST(BtreePage, get_ith_cell_offset_2CellsTable)
{
  FILE *f_db = open_sqlite_db("t/db/BtreePage-2cells-table.sqlite");
  ASSERT_TRUE(f_db);

  DbHeader db_header(f_db);
  ASSERT_TRUE(db_header.read());

  TBtreePage btree_page(f_db, &db_header, 2);
  ASSERT_TRUE(btree_page.read());

  {
    ASSERT_GT(btree_page.get_ith_cell_offset(0), 0);
    ASSERT_LT(btree_page.get_ith_cell_offset(0), db_header.get_pg_sz());
  }
  {
    ASSERT_GT(btree_page.get_ith_cell_offset(1), 0);
    ASSERT_LT(btree_page.get_ith_cell_offset(1), db_header.get_pg_sz());
  }
  {
    ASSERT_EQ(btree_page.get_ith_cell_offset(2), 0);
  }

  fclose(f_db);
}


/*
** TableLeafPage
*/
TEST(TableLeafPage, get_ith_cell_cols_2CellsTable)
{
  FILE *f_db = open_sqlite_db("t/db/TableLeafPage-int.sqlite");
  ASSERT_TRUE(f_db);

  DbHeader db_header(f_db);
  ASSERT_TRUE(db_header.read());

  TableLeafPage tbl_leaf_page(f_db, &db_header, 2);
  ASSERT_TRUE(tbl_leaf_page.read());

  {
    for (u64 row = 0; row < 2; ++row) {
      u64 rowid;
      Pgno overflow_pgno;
      u64 overflown_payload_sz;
      vector<Pgsz> cols_offset, cols_len;
      vector<sqlite_type> cols_type;

      tbl_leaf_page.get_ith_cell_cols(
        row,
        &rowid,
        &overflow_pgno, &overflown_payload_sz,
        cols_offset,
        cols_len,
        cols_type);

      ASSERT_EQ(rowid, row + 1);

      for (u64 col = 0; col < 2; ++col) {
        if (row == 0 && col == 0) {
          // ST_C1 is used for value `1' instead of ST_INT8,
          // which has 0 length and can be specified only by stype.
          ASSERT_EQ(cols_type[col], ST_C1);
          ASSERT_EQ(cols_len[col], 0);
        }
        else {
          ASSERT_EQ(cols_type[col], ST_INT8);
          ASSERT_EQ(2*row + col + 1,
                    u8s_to_val<u8>(&tbl_leaf_page.pg_data[cols_offset[col]], cols_len[col]));
        }
      }
    }
  }

  fclose(f_db);
}
TEST(TableLeafPage, get_ith_cell_cols_GetTableSchema)
{
  FILE *f_db = open_sqlite_db("t/db/TableLeafPage-2tables.sqlite");
  ASSERT_TRUE(f_db);

  DbHeader db_header(f_db);
  ASSERT_TRUE(db_header.read());

  TableLeafPage tbl_leaf_page(f_db, &db_header, 1); // sqlite_master
  ASSERT_TRUE(tbl_leaf_page.read());

  {
    for (u64 row = 0; row < 2; ++row) {
      u64 rowid;
      Pgno overflow_pgno;
      u64 overflown_payload_sz;
      vector<Pgsz> cols_offset, cols_len;
      vector<sqlite_type> cols_type;

      tbl_leaf_page.get_ith_cell_cols(
        row,
        &rowid,
        &overflow_pgno, &overflown_payload_sz,
        cols_offset,
        cols_len,
        cols_type);

      ASSERT_EQ(rowid, row + 1);

      ASSERT_EQ(cols_type[SQLITE_MASTER_COLNO_SQL], ST_TEXT);
      string data((char *)&tbl_leaf_page.pg_data[cols_offset[SQLITE_MASTER_COLNO_SQL]],
                  cols_len[SQLITE_MASTER_COLNO_SQL]);
      char answer[100];
      sprintf(answer, "CREATE TABLE t%llu (c1 INT, c2 INT)", rowid);
      ASSERT_STREQ(data.c_str(), answer);
    }
  }

  fclose(f_db);
}
TEST(TableLeafPage, DISABLED_get_ith_cell_cols_OverflowPage)
{
  // TODO: write
}


// TableBtree
TEST(TableBtree, get_record_by_key_NoInteriorPage)
{
  FILE *f_db = open_sqlite_db("t/db/TableBtree-NoInteriorPage.sqlite");
  ASSERT_TRUE(f_db);

  TableBtree tbl_btree(f_db);
  {
    Pgno rec_pgno;
    Pgsz ith_cell_in_pg;
    ASSERT_TRUE(tbl_btree.get_cell_by_key(f_db, 1, 1, &rec_pgno, &ith_cell_in_pg));
    ASSERT_EQ(rec_pgno, 1u);
    ASSERT_EQ(ith_cell_in_pg, 0);
  }
  {
    Pgno rec_pgno;
    Pgsz ith_cell_in_pg;
    ASSERT_TRUE(tbl_btree.get_cell_by_key(f_db, 1, 2, &rec_pgno, &ith_cell_in_pg));
    ASSERT_EQ(rec_pgno, 1u);
    ASSERT_EQ(ith_cell_in_pg, 1u);
  }

  fclose(f_db);
}

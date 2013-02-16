#include <gtest/gtest.h>

#include "sqlite_format.h"


/*
** BtreePage
*/
class TBtreePage : public BtreePage {
public:
  TBtreePage(FILE * const f_db,
             const DbHeader * const db_header,
             u32 pg_id)
    : BtreePage(f_db, db_header, pg_id)
  {}
  u16 get_ith_cell_offset(u16 i) {
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
TEST(BtreePage, BtreePageValidityCheck_fail)
{
  FILE *f_db = open_sqlite_db("t/db/BtreePage-empty-table.sqlite");
  ASSERT_TRUE(f_db);

  DbHeader db_header(f_db);
  ASSERT_TRUE(db_header.read());

  TBtreePage btree_page(f_db, &db_header, 1);
  ASSERT_FALSE(btree_page.read());
  ASSERT_FALSE(btree_page.is_valid_hdr());

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
      u32 overflow_pgno;
      u64 overflown_payload_sz;
      vector<u16> cells_offset, cells_len;
      vector<sqlite_type> cells_type;

      tbl_leaf_page.get_icell_cols(
        row,
        &rowid,
        &overflow_pgno, &overflown_payload_sz,
        cells_offset,
        cells_len,
        cells_type);

      ASSERT_EQ(rowid, row + 1);

      for (u64 col = 0; col < 2; ++col) {
        if (row == 0 && col == 0) {
          // ST_C1 is used for value `1' instead of ST_INT8,
          // which has 0 length and can be specified only by stype.
          ASSERT_EQ(cells_type[col], ST_C1);
          ASSERT_EQ(cells_len[col], 0);
        }
        else {
          ASSERT_EQ(cells_type[col], ST_INT8);
          ASSERT_EQ(2*row + col + 1,
                    u8s_to_val<u8>(&tbl_leaf_page.pg_data[cells_offset[col]], cells_len[col]));
        }
      }
    }
  }

  fclose(f_db);
}
TEST(TableLeafPage, DISABLED_get_ith_cell_cols_OverflowPage)
{
  // TODO: write
}

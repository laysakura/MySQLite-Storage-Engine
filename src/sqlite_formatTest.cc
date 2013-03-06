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
TEST(TableLeafPage, get_ith_cell_2CellsTable)
{
  FILE *f_db = open_sqlite_db("t/db/TableLeafPage-int.sqlite");
  ASSERT_TRUE(f_db);

  DbHeader db_header(f_db);
  ASSERT_TRUE(db_header.read());

  TableLeafPage tbl_leaf_page(f_db, &db_header, 2);
  ASSERT_TRUE(tbl_leaf_page.read());

  {
    for (u64 row = 0; row < 2; ++row) {
      TableLeafPageCell cell;

      ASSERT_TRUE(tbl_leaf_page.get_ith_cell(row, &cell));
      ASSERT_EQ(cell.rowid, row + 1);
      ASSERT_EQ(cell.overflow_pgno, 0u);

      for (u64 col = 0; col < 2; ++col) {
        if (row == 0 && col == 0) {
          // ST_C1 is used for value `1' instead of ST_INT8,
          // which has 0 length and can be specified only by stype.
          ASSERT_EQ(cell.payload.cols_type[col], ST_C1);
          ASSERT_EQ(cell.payload.cols_len[col], 0);
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

  fclose(f_db);
}
TEST(TableLeafPage, get_ith_cell_GetTableSchema)
{
  FILE *f_db = open_sqlite_db("t/db/TableLeafPage-2tables.sqlite");
  ASSERT_TRUE(f_db);

  DbHeader db_header(f_db);
  ASSERT_TRUE(db_header.read());

  TableLeafPage tbl_leaf_page(f_db, &db_header, 1); // sqlite_master
  ASSERT_TRUE(tbl_leaf_page.read());

  {
    for (u64 row = 0; row < 2; ++row) {
      TableLeafPageCell cell;

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

  fclose(f_db);
}
TEST(TableLeafPage, get_ith_cell_OverflowPage)
{
  FILE *f_db = open_sqlite_db("t/db/TableLeafPage-overflowpage.sqlite");
  ASSERT_TRUE(f_db);

  DbHeader db_header(f_db);
  ASSERT_TRUE(db_header.read());

  TableLeafPage tbl_leaf_page(f_db, &db_header, 2);
  ASSERT_TRUE(tbl_leaf_page.read());

  {
    TableLeafPageCell cell;

    ASSERT_TRUE(tbl_leaf_page.get_ith_cell(0, &cell));
    ASSERT_EQ(cell.rowid, 1u);
    ASSERT_NE(cell.overflow_pgno, 0u);

    ASSERT_EQ(cell.payload.cols_type[0], ST_TEXT);
    string data((char *)&cell.payload.data[cell.payload.cols_offset[0]],
                cell.payload.cols_len[0]);
    string answer(1000, 'a');
    ASSERT_STREQ(data.c_str(), answer.c_str());
  }

  fclose(f_db);
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
    ASSERT_EQ(ith_cell_in_pg, 1);
  }
  {
    Pgno rec_pgno;
    Pgsz ith_cell_in_pg;
    ASSERT_FALSE(tbl_btree.get_cell_by_key(f_db, 1, 3, &rec_pgno, &ith_cell_in_pg));
  }

  fclose(f_db);
}
TEST(TableBtree, get_record_by_key_NoInteriorPage_fragmented)
{
  FILE *f_db = open_sqlite_db("t/db/TableBtree-NoInteriorPage-fragmented.sqlite");
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
    ASSERT_FALSE(tbl_btree.get_cell_by_key(f_db, 1, 2, &rec_pgno, &ith_cell_in_pg));
  }
  {
    Pgno rec_pgno;
    Pgsz ith_cell_in_pg;
    ASSERT_TRUE(tbl_btree.get_cell_by_key(f_db, 1, 3, &rec_pgno, &ith_cell_in_pg));
    ASSERT_EQ(rec_pgno, 1u);
    ASSERT_EQ(ith_cell_in_pg, 1);
  }

  fclose(f_db);
}
TEST(TableBtree, get_record_by_key_10000rec_4tab_4096psize)
{
  FILE *f_db = open_sqlite_db("t/db/TableBtree-10000rec-4tab-4096psize.sqlite");
  ASSERT_TRUE(f_db);

  TableBtree tbl_btree(f_db);
  {
    Pgno rec_pgno;
    Pgsz ith_cell_in_pg;
    ASSERT_TRUE(tbl_btree.get_cell_by_key(f_db, 2, 1, &rec_pgno, &ith_cell_in_pg));
    ASSERT_EQ(rec_pgno, 6u);
    ASSERT_EQ(ith_cell_in_pg, 0);
  }
  {
    Pgno rec_pgno;
    Pgsz ith_cell_in_pg;
    ASSERT_TRUE(tbl_btree.get_cell_by_key(f_db, 2, 200, &rec_pgno, &ith_cell_in_pg));
    ASSERT_EQ(rec_pgno, 6u);
    ASSERT_EQ(ith_cell_in_pg, 199);
  }
  {
    Pgno rec_pgno;
    Pgsz ith_cell_in_pg;
    ASSERT_TRUE(tbl_btree.get_cell_by_key(f_db, 2, 421, &rec_pgno, &ith_cell_in_pg));
    ASSERT_EQ(rec_pgno, 6u);
    ASSERT_EQ(ith_cell_in_pg, 420);
  }
  {
    Pgno rec_pgno;
    Pgsz ith_cell_in_pg;
    ASSERT_TRUE(tbl_btree.get_cell_by_key(f_db, 2, 422, &rec_pgno, &ith_cell_in_pg));
    ASSERT_EQ(rec_pgno, 7u);
    ASSERT_EQ(ith_cell_in_pg, 0);
  }
  {
    Pgno rec_pgno;
    Pgsz ith_cell_in_pg;
    ASSERT_TRUE(tbl_btree.get_cell_by_key(f_db, 2, 2500, &rec_pgno, &ith_cell_in_pg));
    ASSERT_EQ(rec_pgno, 30u);
    ASSERT_EQ(ith_cell_in_pg, 38);
  }
  {
    Pgno rec_pgno;
    Pgsz ith_cell_in_pg;
    ASSERT_FALSE(tbl_btree.get_cell_by_key(f_db, 2, 2501, &rec_pgno, &ith_cell_in_pg));
  }

  fclose(f_db);
}

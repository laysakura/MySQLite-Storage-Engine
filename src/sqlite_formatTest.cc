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
    ASSERT_LT(btree_page.get_ith_cell_offset(0), db_header.get_pg_size());
  }
  {
    ASSERT_GT(btree_page.get_ith_cell_offset(1), 0);
    ASSERT_LT(btree_page.get_ith_cell_offset(1), db_header.get_pg_size());
  }
  {
    ASSERT_EQ(btree_page.get_ith_cell_offset(2), 0);
  }

  fclose(f_db);
}


/*
** TableLeafPage
*/
TEST(TableLeafPage, get_ith_cell_offset_2CellsTable)
{
  FILE *f_db = open_sqlite_db("t/db/TableLeafPage-int.sqlite");
  ASSERT_TRUE(f_db);

  DbHeader db_header(f_db);
  ASSERT_TRUE(db_header.read());

  TableLeafPage tbl_leaf_page(f_db, &db_header, 2);
  ASSERT_TRUE(tbl_leaf_page.read());

  {
    u16 offset, len;
    sqlite_type type;

    tbl_leaf_page.get_icell_jcol_data(0, 0, &offset, &len, &type);
    ASSERT_EQ(type, ST_INT8);
    ASSERT_EQ(1, u8s_to_val<u8>(&tbl_leaf_page.pg_data[offset], len));

    tbl_leaf_page.get_icell_jcol_data(0, 1, &offset, &len, &type);
    ASSERT_EQ(type, ST_INT8);
    ASSERT_EQ(2, u8s_to_val<u8>(&tbl_leaf_page.pg_data[offset], len));

    tbl_leaf_page.get_icell_jcol_data(1, 0, &offset, &len, &type);
    ASSERT_EQ(type, ST_INT8);
    ASSERT_EQ(3, u8s_to_val<u8>(&tbl_leaf_page.pg_data[offset], len));

    tbl_leaf_page.get_icell_jcol_data(1, 1, &offset, &len, &type);
    ASSERT_EQ(type, ST_INT8);
    ASSERT_EQ(4, u8s_to_val<u8>(&tbl_leaf_page.pg_data[offset], len));
  }

  fclose(f_db);
}

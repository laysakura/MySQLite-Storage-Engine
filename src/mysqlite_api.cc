using namespace std;
#include <string>
#include "mysqlite_api.h"


namespace mysqlite {

/***********************************************************************
** Connection class
***********************************************************************/
errstat Connection::open(const char * const db_path)
{
  errstat res;
  f_db = open_sqlite_db(db_path, &res);
  if (res == MYSQLITE_OK) {
    assert(f_db);
    return res;
  }
  else {
    log_errstat(res);
    return res;
  }
}

void Connection::close()
{
  fclose(f_db);
}

RowCursor *Connection::table_fullscan(const char * const table)
{
  // Find root pgno of table from sqlite_master
  Pgno root_pgno = 0;
  RowCursor *sqlite_master_rows = table_fullscan(SQLITE_MASTER_ROOTPGNO);
  while (sqlite_master_rows->next()) {
    const char *tbl_name =
      sqlite_master_rows->get_text(SQLITE_MASTER_COLNO_TBL_NAME);
    if (0 == strcmp(table, tbl_name)) {
      root_pgno = sqlite_master_rows->get_int(SQLITE_MASTER_COLNO_ROOTPAGE);
      break;
    }
  }
  if (root_pgno == 0) {
    log_errstat(MYSQLITE_NO_SUCH_TABLE);
    return NULL;
  }

  return table_fullscan(root_pgno);
}

/*
** Returns RowCursor to start traversing table B-tree
*/
RowCursor *Connection::table_fullscan(Pgno tbl_root)
{
  DbHeader db_header(f_db);
  errstat res;
  if (MYSQLITE_OK != (res = db_header.read())) {
    log_errstat(res);
    return NULL;
  }
  return new FullscanCursor(f_db, tbl_root);
}


/***********************************************************************
** RowCursor class
***********************************************************************/
RowCursor::RowCursor(FILE *f_db, Pgno root_pgno)
  : f_db(f_db),
    visit_path(1, BtreePathNode(root_pgno, 0)), cpa_idx(-1)
{
  assert(f_db);
}

mysqlite_type RowCursor::get_type(int colno) const
{
  // TODO: Now both get_type and get_(int|text|...) materializes RecordCell.
  // TODO: So redundunt....
  // TODO: use cache for both record and page!!
  DbHeader db_header(f_db);
  assert(MYSQLITE_OK == db_header.read());

  TableLeafPage tbl_leaf_page(f_db, &db_header, visit_path.back().pgno);
  assert(MYSQLITE_OK == tbl_leaf_page.read());

  RecordCell cell;
  if (!tbl_leaf_page.get_ith_cell(cpa_idx, &cell) &&
      cell.has_overflow_pg()) {  //オーバフローページのために毎回こんなこと書かなきゃいけないのって割とこわい
    u8 *payload_data = new u8[cell.payload_sz];
    assert(tbl_leaf_page.get_ith_cell(cpa_idx, &cell, payload_data));
  }

  return sqlite_type_to_mysqlite_type(cell.payload.cols_type[colno]);
}

int RowCursor::get_int(int colno) const
{
  // TODO: use cache for both record and page!!
  DbHeader db_header(f_db);
  errstat res;
  if (MYSQLITE_OK != (res = db_header.read())) {
    log_errstat(res);
    return 0;
  }

  TableLeafPage tbl_leaf_page(f_db, &db_header, visit_path.back().pgno);
  assert(MYSQLITE_OK == tbl_leaf_page.read());

  RecordCell cell;
  if (!tbl_leaf_page.get_ith_cell(cpa_idx, &cell) &&
      cell.has_overflow_pg()) {  //オーバフローページのために毎回こんなこと書かなきゃいけないのって割とこわい
    u8 *payload_data = new u8[cell.payload_sz];
    assert(tbl_leaf_page.get_ith_cell(cpa_idx, &cell, payload_data));
  }
  return u8s_to_val<int>(&cell.payload.data[cell.payload.cols_offset[colno]],
                         cell.payload.cols_len[colno]);
}
const char *RowCursor::get_text(int colno) const
{
  // TODO: use cache for both record and page!!
  DbHeader db_header(f_db);
  errstat res;
  if (MYSQLITE_OK != (res = db_header.read())) {
    log_errstat(res);
    return 0;
  }

  TableLeafPage tbl_leaf_page(f_db, &db_header, visit_path.back().pgno);
  assert(MYSQLITE_OK == tbl_leaf_page.read());

  RecordCell cell;
  if (!tbl_leaf_page.get_ith_cell(cpa_idx, &cell) &&
      cell.has_overflow_pg()) {  //オーバフローページのために毎回こんなこと書かなきゃいけないのって割とこわい
    u8 *payload_data = new u8[cell.payload_sz];
    assert(tbl_leaf_page.get_ith_cell(cpa_idx, &cell, payload_data));
  }

  // TODO: Cache... now memory leak
  string *ret = new string((char *)&cell.payload.data[cell.payload.cols_offset[colno]],
                          cell.payload.cols_len[colno]);  //これもシンタックスシュガーが欲しい
  return ret->c_str();
}


/***********************************************************************
** FullscanCursor class
***********************************************************************/
FullscanCursor::FullscanCursor(FILE *f_db, Pgno root_pgno)
  : RowCursor(f_db, root_pgno)
{
}

FullscanCursor::~FullscanCursor()
{
}

void FullscanCursor::close()
{
  delete this;
}

bool FullscanCursor::next()
{
  DbHeader db_header(f_db);
  errstat res;
  if (MYSQLITE_OK != (res = db_header.read())) {
    log_errstat(res);
    return false;
  }

  BtreePage *cur_page = new BtreePage(f_db, &db_header,
                                      visit_path.back().pgno);
  assert(MYSQLITE_OK == cur_page->read());  // TODO: Cache

  btree_page_type btree_type = cur_page->get_btree_type();
  assert(btree_type == TABLE_LEAF || btree_type == TABLE_INTERIOR);

  // If at interior node, then to leftmost leaf
  if (btree_type == TABLE_INTERIOR) {
    TableInteriorPage *cur_interior_page = static_cast<TableInteriorPage *>(cur_page);
    vector<BtreePathNode> path;
    assert(cur_interior_page->get_path_to_leftmost_leaf(&path));
    visit_path.insert(
                     visit_path.end(),
                               path.begin(), path.end()
                               );

    delete cur_page;
    cur_page = new BtreePage(f_db, &db_header,
                             visit_path.back().pgno);
  }
  assert(cur_page->get_btree_type() == TABLE_LEAF);

  // get a cell
  bool has_cell = cur_page->has_ith_cell(++cpa_idx);
  if (has_cell) {
    // cur_page has cell to point at
    return true;
  }
  else if (!has_cell && visit_path.size() == 1) {
    // leaf == root. And no more cell
    return false;
  }
  else if (!has_cell && visit_path.size() > 1) {
    // Jump to parent who has more children
    while (visit_path.size() > 1) {
      BtreePathNode child_node = visit_path.back();
      visit_path.pop_back();
      BtreePathNode parent_node = visit_path.back();

      TableInteriorPage cur_interior_page(f_db, &db_header, parent_node.pgno);
      if (cur_interior_page.has_ith_cell(child_node.sib_idx + 1)) {
        // parent has next child
        break;
      }
    }
    // go to leftmost leaf
    TableInteriorPage *cur_interior_page = new TableInteriorPage(f_db, &db_header,
                                                                 visit_path.back().pgno);
    vector<BtreePathNode> path;
    assert(cur_interior_page->get_path_to_leftmost_leaf(&path));
    delete cur_interior_page;

    visit_path.insert(
                               visit_path.end(),
                               path.begin(), path.end()
                               );

    return next();
  }
  else assert(0);
}


/***********************************************************************
** Functions
***********************************************************************/

}

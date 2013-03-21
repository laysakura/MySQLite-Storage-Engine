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
  if (res == MYSQLITE_OK || res == MYSQLITE_DB_FILE_NOT_FOUND) {
    assert(f_db);
    return res;
  } else {
    log_errstat(res);
    return res;
  }
}

bool Connection::is_opened() const
{
  return f_db != NULL;
}

void Connection::close()
{
  fclose(f_db);
}

RowCursor *Connection::table_fullscan(const char * const table)
{
  Pgno root_pgno = 0;
  if (0 == strcmp("sqlite_master", table)) {
    root_pgno = SQLITE_MASTER_ROOTPGNO;
  }
  else {
    // Find root pgno of table from sqlite_master.
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

/*
  Find a next table leaf cell (record)
  from a table whose root pgno is visit_path[0].pgno.

  This function is called recursively by itself.
  Every call to next() follows to return (return next()).
  Since (interior|leaf) cells who have been visited are not
  visited again, every call to next() has different RowCursor state.

  (1) When visit_path.back().pgno points at leaf page:
    - (1-1) If the leaf has more cell (record),
        point the cell by cpa_idx and return true.
    - (1-2) If the leaf does not have any cell (record),
        jump back to the direct parent (table interior) node
        by visit_path.pop_back() and call next() recursively.
        Or if the leaf has no direct parent,
        return false since all records are already fetched.
  (2) When visit_path.back().pgno points at interior page:
    - (2-1) If the interior has more left child cell or rightmost child,
        jump to the child by visit_path.push_back()
        and call next() recursively.
    - (2-2) If the interior has no more child,
        jump back to the direct parent (table interior) node
        by visit_path.pop_back() and call next() recursively.
        Or if the leaf has no direct parent,
        return false since all records are already fetched.

  Here, the procedure
  "jump back to the direct parent (table interior) node
   by visit_path.pop_back() and call next() recursively.
   Or if the leaf has no direct parent,
   return false since all records are already fetched."
  is encapsulated as jump_to_parent_or_finish_traversal().
*/
bool FullscanCursor::next()
{
  DbHeader db_header(f_db);
  errstat res;
  if (MYSQLITE_OK != (res = db_header.read())) {
    log_errstat(res);
    return false;
  }

  BtreePage cur_page(f_db, &db_header, visit_path.back().pgno);
  assert(MYSQLITE_OK == cur_page.read());  // TODO: Cache

  if (TABLE_LEAF == cur_page.get_btree_type()) {
    // (1) At leaf node,
    TableLeafPage *cur_leaf_page = static_cast<TableLeafPage *>(&cur_page);
    bool has_cell = cur_leaf_page->has_ith_cell(++cpa_idx);
    if (has_cell) {
      // (1-1) The leaf has more cell
      return true;
    } else {
      // (1-2) The leaf has no more cell
      cpa_idx = -1;
      return jump_to_parent_or_finish_traversal() ?
        next() : false;
    }
  }
  else if (TABLE_INTERIOR == cur_page.get_btree_type()) {
    // (2) At interior node,
    TableInteriorPage *cur_interior_page = static_cast<TableInteriorPage *>(&cur_page);
    Pgsz n_cell = cur_interior_page->get_n_cell();

    if ((int)visit_path.back().child_idx_to_visit < n_cell) {
      // (2-1) The interior has left child cell
      struct TableInteriorPageCell cell;
      cur_interior_page->get_ith_cell(visit_path.back().child_idx_to_visit,
                                      &cell);
      visit_path.push_back(BtreePathNode(cell.left_child_pgno, 0));
      return next();
    } else if ((int)visit_path.back().child_idx_to_visit == n_cell) {
      // (2-1) The interior has rightmost child
      visit_path.push_back(BtreePathNode(cur_interior_page->get_rightmost_pg(), 0));
      return next();
    } else {
      // (2-2) The interior has no more child
      return jump_to_parent_or_finish_traversal() ?
        next() : false;
    }
  }
  else abort();  // cur_page.get_btree_type() == TABLE_LEAF || TABLE_INTERIOR
}

bool FullscanCursor::jump_to_parent_or_finish_traversal()
{
  assert(visit_path.size() >= 1);
  if (visit_path.size() == 1) return false;  // finish traversal

  visit_path.pop_back();
  ++visit_path.back().child_idx_to_visit;
  return true;
}


/***********************************************************************
** Functions
***********************************************************************/

}

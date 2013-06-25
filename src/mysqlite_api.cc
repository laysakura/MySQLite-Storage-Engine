using namespace std;
#include <string>
#include <fcntl.h>
#include "mysqlite_api.h"
#include "pcache.h"


namespace mysqlite {

/***********************************************************************
** Connection class
***********************************************************************/
errstat Connection::open(const char * const db_path)
{
  errstat res;

  if (is_opened()) {
    return MYSQLITE_CONNECTION_ALREADY_OPEN;
  }

  if (f_db) {
    fclose(f_db);
    f_db = NULL;
  }
  f_db = open_sqlite_db(db_path, &res);

  if (res == MYSQLITE_OK) {
    // DB file exists on db_path
    is_opened_flag = true;
  } else if (res == MYSQLITE_DB_FILE_NOT_FOUND) {
    // TODO: Newly creating database file
    is_opened_flag = true;
  } else {
    // error
    log_errstat(res);
    return res;
  }

  // Page cache
  PageCache *pcache = PageCache::get_instance();
  res = pcache->refresh(f_db);

  if (res == MYSQLITE_OK) {
    // succeeded in opening db_path (read-mode or write-mode)
  } else {
    // failed in opening db_path
    log_errstat(res);
  }

  return res;
}

bool Connection::is_opened() const
{
  return is_opened_flag;
}

void Connection::close()
{
  is_opened_flag = false;
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
      string tbl_name =
        sqlite_master_rows->get_text(SQLITE_MASTER_COLNO_TBL_NAME);
      if (0 == strcmp(table, tbl_name.c_str())) {
        root_pgno = sqlite_master_rows->get_int(SQLITE_MASTER_COLNO_ROOTPAGE);
        break;
      }
    }
    if (root_pgno == 0) {
      log_errstat(MYSQLITE_NO_SUCH_TABLE);
      return NULL;
    }
    sqlite_master_rows->close();
  }

  return table_fullscan(root_pgno);
}

/*
** Returns RowCursor to start traversing table B-tree
*/
RowCursor *Connection::table_fullscan(Pgno tbl_root)
{
  return new FullscanCursor(tbl_root);
}

int Connection::rdlock_db()
{
  unsigned int refcnt = __sync_fetch_and_add(&refcnt_rdlock_db, 1);
  if (refcnt == 0) {
    // first thread requests lock
    struct flock flock;
    flock.l_whence = SEEK_SET;
    flock.l_start = 0;
    flock.l_len = 0;
    flock.l_type = F_RDLCK;
    fcntl(fileno(f_db), F_SETLKW, &flock);  // always returns 0

    log_msg("Connection::rdlock_db(): Thread#%lu locks db file",
            pthread_self());
  }
  return 0;
}

int Connection::unlock_db()
{
  unsigned int refcnt = __sync_fetch_and_sub(&refcnt_rdlock_db, 1);
  assert(refcnt > 0);
  if (refcnt == 1) {
    // last thread requests lock
    struct flock flock;
    flock.l_whence = SEEK_SET;
    flock.l_start = 0;
    flock.l_len = 0;
    flock.l_type = F_UNLCK;
    fcntl(fileno(f_db), F_SETLKW, &flock);  // always returns 0

    log_msg("Connection::rdlock_db(): Thread#%lu locks db file",
            pthread_self());
  }

  return 0;
}


/***********************************************************************
** RowCursor class
***********************************************************************/
RowCursor::RowCursor(Pgno root_pgno)
  : visit_path(1, BtreePathNode(root_pgno, 0)), cpa_idx(-1)
{
}

mysqlite_type RowCursor::get_type(int colno) const
{
  // TODO: Now both get_type and get_(int|text|...) materializes RecordCell.
  // TODO: So redundunt....
  // TODO: use cache for record!!
  TableLeafPage tbl_leaf_page(visit_path.back().pgno);
  errstat ret = tbl_leaf_page.fetch();
  my_assert(ret == MYSQLITE_OK);

  RecordCell cell;
  if (!tbl_leaf_page.get_ith_cell(cpa_idx, &cell) &&
      cell.has_overflow_pg()) {  //オーバフローページのために毎回こんなこと書かなきゃいけないのって割とこわい
    u8 *payload_data = new u8[cell.payload_sz];
    bool ret = tbl_leaf_page.get_ith_cell(cpa_idx, &cell, payload_data);
    my_assert(ret);
  }

  return sqlite_type_to_mysqlite_type(cell.payload.cols_type[colno]);
}

int RowCursor::get_int(int colno) const
{
  // TODO: use cache for record!!
  TableLeafPage tbl_leaf_page(visit_path.back().pgno);
  errstat ret = tbl_leaf_page.fetch();
  my_assert(ret == MYSQLITE_OK);

  RecordCell cell;
  if (!tbl_leaf_page.get_ith_cell(cpa_idx, &cell) &&
      cell.has_overflow_pg()) {  //オーバフローページのために毎回こんなこと書かなきゃいけないのって割とこわい
    u8 *payload_data = new u8[cell.payload_sz];
    bool ret = tbl_leaf_page.get_ith_cell(cpa_idx, &cell, payload_data);
    my_assert(ret);
  }
  if (cell.payload.cols_type[colno] == ST_C0) {
    return 0;
  }
  else if (cell.payload.cols_type[colno] == ST_C1) {
    return 1;
  }
  else {
    return u8s_to_val<int>(&cell.payload.data[cell.payload.cols_offset[colno]],
                           cell.payload.cols_len[colno]);
  }
}
string RowCursor::get_text(int colno) const
{
  // TODO: use cache for record!!
  TableLeafPage tbl_leaf_page(visit_path.back().pgno);
  errstat res = tbl_leaf_page.fetch();
  my_assert(res == MYSQLITE_OK);

  RecordCell cell;
  if (!tbl_leaf_page.get_ith_cell(cpa_idx, &cell) &&
      cell.has_overflow_pg()) {  //オーバフローページのために毎回こんなこと書かなきゃいけないのって割とこわい
    u8 *payload_data = new u8[cell.payload_sz];
    bool ret = tbl_leaf_page.get_ith_cell(cpa_idx, &cell, payload_data);
    my_assert(ret);
  }

  // TODO: Cache... now memory leak
  return string((char *)&cell.payload.data[cell.payload.cols_offset[colno]],
                cell.payload.cols_len[colno]);  //これもシンタックスシュガーが欲しい
}


/***********************************************************************
** FullscanCursor class
***********************************************************************/
FullscanCursor::FullscanCursor(Pgno root_pgno)
  : RowCursor(root_pgno)
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
  BtreePage cur_page(visit_path.back().pgno);
  errstat ret = cur_page.fetch();
  my_assert(ret == MYSQLITE_OK);

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
  my_assert(visit_path.size() >= 1);
  if (visit_path.size() == 1) return false;  // finish traversal

  visit_path.pop_back();
  ++visit_path.back().child_idx_to_visit;
  return true;
}


/***********************************************************************
** Functions
***********************************************************************/

}

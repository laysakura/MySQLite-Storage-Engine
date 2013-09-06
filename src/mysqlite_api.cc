using namespace std;
#include <string>
#include <fcntl.h>
#include "mysqlite_api.h"
#include "pcache.h"


namespace mysqlite {

/***********************************************************************
** Connection class
***********************************************************************/
Connection::~Connection()
{
  PageCache *pcache = PageCache::get_instance();
  pcache->close();
}

// TODO: *****MOST IMPORTANT*****
// - Each thread have the same number of Connection instance as open DB.
// - Page cache is shared across threads and is maintained by reference count.
errstat Connection::open(const char * const db_path)
{
  errstat res;

  if (is_opened()) {
    PageCache *pcache = PageCache::get_instance();
    pcache->close();
  }

  // Page cache
  PageCache *pcache = PageCache::get_instance();
  res = pcache->open(db_path);

  if (res == MYSQLITE_OK) {
    // succeeded in opening db_path (read-mode or write-mode)
  } else {
    // failed in opening db_path
    log_errstat(res);
  }

  return res;
}

bool Connection:: is_opened() const
{
  PageCache *pcache = PageCache::get_instance();
  return pcache->is_opened();
}

RowCursor *Connection::table_fullscan(const char * const table)
{
  Pgno root_pgno = 0;
  if (0 == strcmp("sqlite_master", table)) {
    root_pgno = SQLITE_MASTER_ROOTPGNO;
  }
  else {
    // Find root pgno of table from sqlite_master.
    std::unique_ptr<RowCursor> sqlite_master_rows(table_fullscan(SQLITE_MASTER_ROOTPGNO));
    while (sqlite_master_rows->next()) {
      vector<u8> buf;
      sqlite_master_rows->get_blob(SQLITE_MASTER_COLNO_TBL_NAME, buf);
      if (strlen(table) == buf.size() && 0 == memcmp(table, buf.data(), buf.size())) {
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
  return new FullscanCursor(tbl_root);
}

int Connection::rdlock_db()
{
  PageCache *pcache = PageCache::get_instance();
  pcache->rd_lock();
  // log_msg("Connection::rdlock_db(): Thread#%lu locks db file\n",
  //         pthread_self());
  return 0;
}

int Connection::unlock_db()
{
  PageCache *pcache = PageCache::get_instance();
  pcache->unlock();
  // log_msg("Connection::unlock_db(): Thread#%lu unlocks db file\n",
  //         pthread_self());
  return 0;
}


/***********************************************************************
** RowCursor class
***********************************************************************/
RowCursor::RowCursor(Pgno root_pgno)
  : visit_path(1, BtreePathNode(root_pgno, 0)), cpa_idx(-1), prev_overflown(false), rec_buf(NULL)
{
}
RowCursor::~RowCursor()
{
  if (rec_buf && prev_overflown) free(rec_buf);
}

void FullscanCursor::fill_rec_data()
{
  TableLeafPage tbl_leaf_page(visit_path.back().pgno);
  errstat ret = tbl_leaf_page.fetch();
  my_assert(ret == MYSQLITE_OK);

  cell.payload.clear();  // clear previous record info
  tbl_leaf_page.get_ith_cell(cpa_idx, cell, &rec_buf, prev_overflown);
  assert(rec_buf);
}

mysqlite_type RowCursor::get_type(int colno) const
{
  return sqlite_type_to_mysqlite_type(cell.payload.cols_type[colno]);
}

int RowCursor::get_int(int colno) const
{
  if (cell.payload.cols_type[colno] == ST_C0) {
    return 0;
  }
  else if (cell.payload.cols_type[colno] == ST_C1) {
    return 1;
  }
  else {
    return u8s_to_val<int>(&rec_buf[cell.payload.cols_offset[colno]],
                           cell.payload.cols_len[colno]);
  }
}
void RowCursor::get_blob(int colno,
                         /* out */
                         vector<u8> &buf) const
{
  size_t len = cell.payload.cols_len[colno];
  buf.assign(&rec_buf[cell.payload.cols_offset[colno]], &rec_buf[cell.payload.cols_offset[colno] + len]);
  buf.resize(len);
}
string RowCursor::get_raw_string(int colno) const
{
  vector<u8> buf;
  get_blob(colno, buf);
  /* return string(reinterpret_cast<const char *>(buf.data()), buf.size()); */
  string s(reinterpret_cast<const char *>(buf.data()), buf.size());
  return s;
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
  // delete copied record
  if (prev_overflown) {
    prev_overflown = false;
    free(rec_buf);
    rec_buf = NULL;
  }

  BtreePage cur_page(visit_path.back().pgno);
  errstat ret = cur_page.fetch();
  my_assert(ret == MYSQLITE_OK);

  if (TABLE_LEAF == cur_page.get_btree_type()) {
    // (1) At leaf node,
    TableLeafPage *cur_leaf_page = static_cast<TableLeafPage *>(&cur_page);
    bool has_cell = cur_leaf_page->has_ith_cell(++cpa_idx);
    if (has_cell) {
      // (1-1) The leaf has more cell
      fill_rec_data();
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

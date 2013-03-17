/* Copyright (c) Sho Nakatani 2013. All rights reserved.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */


#ifndef _SQLITE_FORMAT_H_
#define _SQLITE_FORMAT_H_


#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "mysqlite_types.h"
#include "utils.h"


/*
** Constants
*/
#define SQLITE3_SIGNATURE_SZ 16
#define SQLITE3_SIGNATURE "SQLite format 3"

#define DB_HEADER_SZ 100

#define DBHDR_PGSZ_OFFSET 16
#define DBHDR_PGSZ_LEN 2

#define DBHDR_RESERVEDSPACE_OFFSET 20
#define DBHDR_RESERVEDSPACE_LEN 1

#define BTREEHDR_SZ_LEAF 8
#define BTREEHDR_SZ_INTERIOR 12

#define BTREEHDR_BTREETYPE_OFFSET 0

#define BTREEHDR_FREEBLOCKOFST_OFFSET 1
#define BTREEHDR_FREEBLOCKOFST_LEN 2

#define BTREEHDR_NCELL_OFFSET 3
#define BTREEHDR_NCELL_LEN 2

#define BTREEHDR_CELLCONTENTAREAOFST_OFFSET 5
#define BTREEHDR_CELLCONTENTAREAOFST_LEN 2

#define BTREEHDR_NFRAGMENTATION_OFFSET 7

#define BTREEHDR_RIGHTMOSTPG_OFFSET 8
#define BTREEHDR_RIGHTMOSTPG_LEN 4

#define BTREECELL_LECTCHILD_LEN 4
#define BTREECELL_OVERFLOWPGNO_LEN 4

#define CPA_ELEM_LEN 2

#define SQLITE_MASTER_ROOTPGNO 1

#define SQLITE_MASTER_COLNO_TYPE     0
#define SQLITE_MASTER_COLNO_NAME     1
#define SQLITE_MASTER_COLNO_TBL_NAME 2
#define SQLITE_MASTER_COLNO_ROOTPAGE 3
#define SQLITE_MASTER_COLNO_SQL      4

#define SQLITE3_VARINT_MAXLEN 9


/*
** Types
*/
typedef u32 Pgno;
typedef u16 Pgsz;
typedef u64 Rowid;

typedef enum btree_page_type {
  INDEX_INTERIOR     = 2,
  TABLE_INTERIOR     = 5,
  INDEX_LEAF         = 10,
  TABLE_LEAF         = 13,
} btree_page_type;

typedef enum sqlite_type {
  ST_NULL =  0,
  ST_INT8 =  1,
  ST_INT16 = 2,
  ST_INT24 = 3,
  ST_INT32 = 4,
  ST_INT48 = 5,
  ST_INT64 = 6,
  ST_FLOAT = 7,
  ST_C0 =    8,
  ST_C1 =    9,
  ST_BLOB,   /* >= 12, even */
  ST_TEXT,   /* >= 13, odd  */
} sqlite_type;

struct BtreePathNode {
  Pgno pgno;
  Pgno sib_idx;  // 0-origin

  public:
  BtreePathNode(Pgno pgno, Pgno sib_idx) : pgno(pgno), sib_idx(sib_idx) {}
};

struct CellPos {
  Pgsz pgno;  // TODO: obsolete!! remove later

  vector<BtreePathNode> visit_path;  // Save the history of traversal.
               // Example:
               //
               // 0-+-0
               //   |
               //   +-1-+-0
               //   |   |
               //   +-2 +-1
  Pgsz cpa_idx;
  bool cursor_end;  // flag to indicate end of traversing

  public:
  CellPos(Pgno root_pgno)
    : visit_path(1, BtreePathNode(root_pgno, 0)), cpa_idx(-1), cursor_end(false)
  {}

  public:  // TODO: private
  CellPos() {}
};


static inline bool has_sqlite3_signature(FILE * const f)
{
  char s[SQLITE3_SIGNATURE_SZ];
  if (MYSQLITE_OK != mysqlite_fread(s, 0, SQLITE3_SIGNATURE_SZ, f)) return false;
  return strcmp(s, SQLITE3_SIGNATURE) == 0;
}


static inline Pgsz stype2len(Pgsz stype) {
  switch (stype) {
  case ST_NULL:
  case ST_C0:
  case ST_C1:
    return 0;

  case ST_INT8:  return 1;
  case ST_INT16: return 2;
  case ST_INT32: return 4;

  case ST_INT64:
  case ST_FLOAT:
    return 8;
  }
  assert(stype != 10 && stype != 11);
  return (stype - (12 + (stype % 2))) / 2;
}


/*
** Open the path and if file exists in the path then check the validity as a SQLite DB.
**
** @return
** NULL: Error. `message' is set.
*/
static inline FILE *_open_sqlite_db(const char * const path,
                                    /* out */
                                    bool * const is_existing_db,
                                    char * const message,
                                    errstat * const res)
{
  FILE *f;
  struct stat st;
  if (stat(path, &st) == 0) {
    *is_existing_db = true;
    f = fopen(path, "r");
    if (!f) {
      sprintf(message, "Permission denied: Cannot open %s in read mode.", path);
      *res = MYSQLITE_PERMISSION_DENIED;
      return NULL;
    }
    if (!has_sqlite3_signature(f)) {
      sprintf(message, "Format error: %s does not seem SQLite3 database.", path);
      *res = MYSQLITE_CORRUPT_DB;
      return NULL;
    }
  } else {
    *is_existing_db = false;
    f = fopen(path, "w+");
    if (!f) {
      sprintf(message, "Permission denied: Cannot create %s.", path);
      *res = MYSQLITE_PERMISSION_DENIED;
      return NULL;
    }
  }
  *res = MYSQLITE_OK;
  return f;
}
static inline FILE *_open_sqlite_db(const char * const path,
                                    /* out */
                                    bool * const is_existing_db,
                                    errstat * const res)
{
  char msg[1024];
  FILE *f_ret = _open_sqlite_db(path, is_existing_db, msg, res);
  if (!f_ret) log_msg("%s", msg);
  return f_ret;
}
static inline FILE *open_sqlite_db(const char * const existing_path,
                                   /* out */
                                   errstat * const res)
{
  bool is_existing_db;
  FILE *f_ret = _open_sqlite_db(existing_path, &is_existing_db, res);
  if (!f_ret) return NULL;

  if (!is_existing_db) {
    if (0 != fclose(f_ret)) perror("fclose() failed\n");
    *res = MYSQLITE_DB_FILE_NOT_FOUND;
    return NULL;
  }

  return f_ret;
}


/*
** Database header
*/
class DbHeader {
private:
  FILE * const f_db;
  u8 *hdr_data;

  /*
  ** @note
  ** Constructor does not read(2) page contents.
  ** Call this->read() after object is constructed.
  */
  public:
  DbHeader(FILE * const f_db)
    : f_db(f_db)
  {
    assert(f_db);
    assert(hdr_data = (u8 *)malloc(DB_HEADER_SZ));
  }
  public:
  ~DbHeader() {
    free(hdr_data);
  }

  public:
  errstat read() const {
    return mysqlite_fread(hdr_data, 0, DB_HEADER_SZ, f_db);
  }

  public:
  Pgsz get_pg_sz() const {
    return u8s_to_val<Pgsz>(&hdr_data[DBHDR_PGSZ_OFFSET], DBHDR_PGSZ_LEN);
  }

  public:
  Pgsz get_reserved_space() const {
    return u8s_to_val<Pgsz>(&hdr_data[DBHDR_RESERVEDSPACE_OFFSET], DBHDR_RESERVEDSPACE_LEN);
  }

  // Prohibit default constructor
  private:
  DbHeader() : f_db(NULL) {}
};

/*
** Page class
*/
class Page {
protected:
  FILE * const f_db;
public:
  u8 *pg_data;
  const DbHeader * const db_header;
  Pgno pgno;

  /*
  ** @note
  ** Constructor does not read(2) page contents.
  ** Call this->read() after object is constructed.
  */
  public:
  Page(FILE * const f_db,
       const DbHeader * const db_header,
       Pgno pgno)
    : f_db(f_db), db_header(db_header), pgno(pgno)
  {
    assert(f_db);
    assert(db_header);
    assert(pg_data = (u8 *)malloc(db_header->get_pg_sz()));
  }
  public:
  virtual ~Page() {
    free(pg_data);
  }

  public:
  errstat read() const {
    assert(db_header);
    Pgsz pg_sz = db_header->get_pg_sz();
    return mysqlite_fread(pg_data, pg_sz * (pgno - 1), pg_sz, f_db);
  }

  // Prohibit default constructor
  private:
  Page() : f_db(NULL), db_header(NULL) {}
};

/*
** Btree page virtual class
*/
class BtreePage : public Page {
  public:
  BtreePage(FILE * const f_db,
            const DbHeader * const db_header,
            Pgno pgno)
    : Page(f_db, db_header, pgno)
  {}

  // Btree header info

  public:
  btree_page_type get_btree_type() const {
    return (btree_page_type)pg_data[
      pgno == 1 ? DB_HEADER_SZ + BTREEHDR_BTREETYPE_OFFSET :
                   BTREEHDR_BTREETYPE_OFFSET
    ];
  }

  protected:
  Pgsz get_freeblock_offset() const {
    return u8s_to_val<Pgsz>(
      &pg_data[
        pgno == 1 ? DB_HEADER_SZ + BTREEHDR_FREEBLOCKOFST_OFFSET :
                     BTREEHDR_FREEBLOCKOFST_OFFSET
      ],
      BTREEHDR_FREEBLOCKOFST_LEN);
  }

  public:
  Pgsz get_n_cell() const {
    return u8s_to_val<Pgsz>(
      &pg_data[
        pgno == 1 ? DB_HEADER_SZ + BTREEHDR_NCELL_OFFSET :
                     BTREEHDR_NCELL_OFFSET
      ],
      BTREEHDR_NCELL_LEN);
  }

  protected:
  Pgsz get_cell_content_area_offset() const {
    return u8s_to_val<Pgsz>(
      &pg_data[
        pgno == 1 ? DB_HEADER_SZ + BTREEHDR_CELLCONTENTAREAOFST_OFFSET :
                     BTREEHDR_CELLCONTENTAREAOFST_OFFSET
      ],
      BTREEHDR_CELLCONTENTAREAOFST_LEN);
  }

  protected:
  u8 get_n_fragmentation() const {
    return pg_data[
      pgno == 1 ? DB_HEADER_SZ + BTREEHDR_NFRAGMENTATION_OFFSET :
                   BTREEHDR_NFRAGMENTATION_OFFSET
    ];
  }

  public:
  Pgno get_rightmost_pg() const {
    assert(get_btree_type() == INDEX_INTERIOR ||
           get_btree_type() == TABLE_INTERIOR);
    return u8s_to_val<Pgno>(
      &pg_data[
        pgno == 1 ? DB_HEADER_SZ + BTREEHDR_RIGHTMOSTPG_OFFSET :
                     BTREEHDR_RIGHTMOSTPG_OFFSET
      ],
      BTREEHDR_RIGHTMOSTPG_LEN);
  }

  protected:
  bool is_valid_hdr() const {
    bool is_valid = true;
    { // btree type
      btree_page_type type = get_btree_type();
      is_valid &= (type == INDEX_INTERIOR ||
                   type == TABLE_INTERIOR ||
                   type == INDEX_LEAF ||
                   type == TABLE_LEAF);
    }
    { // TODO: freeblock offset check
      /* Pgsz offset = get_freeblock_offset(); */
      /* is_valid &= (0 <= offset && offset < pg_sz); */
    }
    { // TODO: num of cell check
      /* Pgsz n_cell = get_n_cell(); */
      /* is_valid &= (0 <= n_cell && n_cell < pg_sz); */
    }
    { // TODO: cell content area offset check
      /* Pgsz offset = get_cell_content_area_offset(); */
      /* is_valid &= (0 <= offset && offset <= pg_sz); */
      // cell_content_area_offset == pg_sz means
      // there are no cells yet
    }
    { // TODO: num of fragmentation check
      /* Pgsz n_fragmentation = get_n_fragmentation(); */
      /* is_valid &= (0 <= n_fragmentation && n_fragmentation < pg_sz); */
    }
    return is_valid;
  }

  // Cell info

  public:
  bool has_ith_cell(Pgsz i) const {
    return get_ith_cell_offset(i) != 0;
  }

  /*
  ** @param i  0-origin index.
  **
  ** @return  0 if i is larger than the actual number of cells.
  */
  protected:
  Pgsz get_ith_cell_offset(Pgsz i) const {
    if (i >= get_n_cell()) return 0;

    // cpa stands for Cell Pointer Array
    btree_page_type type = get_btree_type();
    Pgsz cpa_start = (type == INDEX_LEAF || type == TABLE_LEAF) ?
      BTREEHDR_SZ_LEAF : BTREEHDR_SZ_INTERIOR;
    if (pgno == 1) cpa_start += DB_HEADER_SZ;
    return u8s_to_val<Pgsz>(&pg_data[cpa_start + CPA_ELEM_LEN * i], CPA_ELEM_LEN);
  }

  protected:
  Pgno get_leftchild_pgno(Pgsz start_offset) const {
    return u8s_to_val<Pgno>(&pg_data[start_offset], BTREECELL_LECTCHILD_LEN);
  }

  /*
  ** @return Payload sz. This can be longer than page sz
  **   when the cell has overflow pages.
  */
  protected:
  u64 get_payload_sz(Pgsz start_offset, u8 *len) const {
    return varint2u64(&pg_data[start_offset], len);
  }

  protected:
  u64 get_rowid(Pgsz start_offset, u8 *len) const {
    return varint2u64(&pg_data[start_offset], len);
  }

  protected:
  Pgno get_overflow_pgno(Pgsz start_offset) const;

  // Page management
  public:
  errstat read() const {
    errstat res = Page::read();
    if (res != MYSQLITE_OK) return res;
    if (!is_valid_hdr()) return MYSQLITE_CORRUPT_DB;
    return MYSQLITE_OK;
  }

};

/*
** Table leaf page
*/
class Payload {
public:
  vector<Pgsz> cols_offset;
  vector<Pgsz> cols_len;
  vector<sqlite_type> cols_type;
  u8 *data;                 // When payload has no overflow page,
                            // it points to a BtreePage's pg_data.
                            // Otherwise, new space is allocated
                            // (by vector class) and raw data is
                            // copied from more than 1 pages.

  public:
  Payload()
    : cols_offset(0), cols_len(0), cols_type(0), data(NULL)
  {}


  /*
  ** Read from this->data and fill cols_*.
  **
  ** @see  Extracting SQLite records - Figure 4. SQLite record format
  */
  public:
  bool digest_data() {
    Pgsz offset = 0;
    u8 len;
    Pgsz hdr_sz = varint2u64(&data[offset], &len);
    offset += len;

    // Read column headers
    for (u64 read_hdr_sz = len; read_hdr_sz < hdr_sz; ) {
      Pgsz stype = varint2u64(&data[offset], &len);
      offset += len;
      read_hdr_sz += len;

      if (stype <= 9) {
        cols_type.push_back(static_cast<sqlite_type>(stype));
      } else if (stype >= 12) {
        cols_type.push_back(stype % 2 == 0 ? ST_BLOB : ST_TEXT);
      } else {
        log_msg("Invalid sqlite type (stype=%d)\n", stype);
        return false;
      }
      cols_len.push_back(stype2len(stype));
    }

    // Read column bodies
    for (vector<Pgsz>::iterator it = cols_len.begin();
         it != cols_len.end();
         ++it)
    {
      cols_offset.push_back(offset);
      offset += *it;
    }
    return true;
  }

};

struct RecordCell {
  u64 payload_sz;
  u64 payload_sz_in_origpg;
  u64 rowid;
  Payload payload;
  u32 overflow_pgno;  // First overflow page num. 0 when cell has no overflow page.

  public:
  inline bool has_overflow_pg() const { return overflow_pgno != 0; }
};

class TableLeafPage : public BtreePage {
  public:
  TableLeafPage(FILE * const f_db,
                const DbHeader * const db_header,
                Pgno pgno)
   : BtreePage(f_db, db_header, pgno)
  {}

  /*
  ** Read i-th cell in this page.
  ** If the cell has overflow page, then cell->payload.data has whole
  ** copied data from pages.
  **
  ** @param i  Specifies i-th cell in the page (0-origin).
  **
  ** @return false on error
  */
  public:
  bool get_ith_cell(Pgsz i,
                    /*out*/
                    RecordCell *cell) const
  {
    u8 len;
    Pgsz cell_offset, offset;
    cell_offset = offset = get_ith_cell_offset(i);
    if (cell_offset == 0) return false;

    cell->payload_sz = get_payload_sz(offset, &len);
    offset += len;

    cell->rowid = get_rowid(offset, &len);
    offset += len;

    cell->payload.data = &pg_data[offset];

    // Overflow page treatment
    // @see  https://github.com/laysakura/SQLiteDbVisualizer/README.org - Track overflow pages
    Pgsz usable_sz = db_header->get_pg_sz() - db_header->get_reserved_space();
    Pgsz max_local = usable_sz - 35;
    if (cell->payload_sz <= max_local) {
      // no overflow page
      cell->overflow_pgno = 0;
    } else {
      // overflow page exists
      Pgsz min_local = (usable_sz - 12) * 32/255 - 23;
      Pgsz local_sz = min_local + (cell->payload_sz - min_local) % (usable_sz - 4);
      cell->payload_sz_in_origpg = local_sz > max_local ? min_local : local_sz;

      cell->overflow_pgno = u8s_to_val<Pgno>(&pg_data[offset + cell->payload_sz_in_origpg],
                                             BTREECELL_OVERFLOWPGNO_LEN);
      return false;  // caller should call
                     // get_ith_cell(Pgsz i, RecordCell *cell,
                     //              vector<u8> *buf_overflown_payload)
                     // later.
    }

    cell->payload.digest_data();
    return true;
  }

  /*
  ** This function is called only if overflow pages exist.
  ** Read i-th cell in this page.
  ** If the cell has overflow page, then cell->payload.data has whole
  ** copied data from pages.
  **
  ** @param i  Specifies i-th cell in the page (0-origin).
  **
  ** @return false on error
  */
  public:
  bool get_ith_cell(Pgsz i,
                    /*out*/
                    RecordCell *cell,
                    u8 *buf_overflown_payload) const
  {
    // Asserted get_ith_cell(Pgsz i, RecordCell *cell) is called first
    assert(buf_overflown_payload);
    assert(cell->overflow_pgno != 0);
    assert(cell->payload_sz_in_origpg > 0);
    assert(cell->payload_sz_in_origpg < cell->payload_sz);

    // Copy payload from this page and overflow pages
    Pgsz offset = 0;
    memcpy(&buf_overflown_payload[offset], cell->payload.data, cell->payload_sz_in_origpg);
    offset += cell->payload_sz_in_origpg;
    Pgsz usable_sz = db_header->get_pg_sz() - db_header->get_reserved_space();
    Pgsz payload_sz_rem = cell->payload_sz - cell->payload_sz_in_origpg;

    for (Pgno overflow_pgno = cell->overflow_pgno; overflow_pgno != 0; ) {
      Page ovpg(f_db, db_header, overflow_pgno);
      assert(MYSQLITE_OK == ovpg.read());
      overflow_pgno = u8s_to_val<Pgno>(&ovpg.pg_data[0], sizeof(Pgno));
      Pgsz payload_sz_inpg = min<Pgsz>(usable_sz - sizeof(Pgno), payload_sz_rem);
      payload_sz_rem -= payload_sz_inpg;
      memcpy(&buf_overflown_payload[offset],
             &ovpg.pg_data[sizeof(Pgno)],
             payload_sz_inpg);
      offset += payload_sz_inpg;
    }
    assert(payload_sz_rem == 0);

    cell->payload.data = buf_overflown_payload;
    cell->payload.digest_data();
    return true;
  }


  /*
  ** @param i  Specifies i-th cell in the page (0-origin).
  */
  public:
  u64 get_ith_cell_rowid(Pgsz i) const
  {
    u8 len;
    Pgsz offset = get_ith_cell_offset(i);
    get_payload_sz(offset, &len);
    offset += len;
    return get_rowid(offset, &len);
  }
};


/*
** Table interior page
*/
struct TableInteriorPageCell {
  Pgno left_child_pgno;
  u64 rowid;
};

class TableInteriorPage : public BtreePage {
  public:
  TableInteriorPage(FILE * const f_db,
                    const DbHeader * const db_header,
                    Pgno pgno)
   : BtreePage(f_db, db_header, pgno)
  {}

  public:
  void get_ith_cell(int i,
                    /* out */
                    struct TableInteriorPageCell *cell) const {
    u8 len;
    Pgsz offset = get_ith_cell_offset(i);

    cell->left_child_pgno = u8s_to_val<Pgno>(&pg_data[offset], sizeof(Pgno));
    offset += sizeof(Pgno);

    cell->rowid = get_rowid(offset, &len);
  }

  /*
  ** 'this' page is not included in 'path'
  */
  public:
  bool get_path_to_leftmost_leaf(/* out */ vector<BtreePathNode> *path) {
    BtreePage *cur_page = this;
    while (cur_page->get_btree_type() == TABLE_INTERIOR) {
      // find leftmost child
      TableInteriorPage *cur_interior_page = static_cast<TableInteriorPage *>(cur_page);
      int n_cell = cur_interior_page->get_n_cell();
      Pgno leftmost_child_pgno;
      if (n_cell > 0) {
        struct TableInteriorPageCell cell;
        cur_interior_page->get_ith_cell(0, &cell);
        leftmost_child_pgno = cell.left_child_pgno;
      } else {
        leftmost_child_pgno = cur_interior_page->get_rightmost_pg();
      }

      // jump to leftmost child
      if (path->size() > 0) {
        // 'this' should not be deleted
        delete cur_page;
      }
      cur_page = new BtreePage(f_db, db_header, leftmost_child_pgno);
      if (MYSQLITE_OK != cur_page->read()) goto read_err;  // TODO: Cache

      // add to path
      ;
      path->push_back(BtreePathNode(cur_page->pgno, 0));
    }
    assert(cur_page->get_btree_type() == TABLE_LEAF);
    return true;

  read_err:
    log_msg("Failed to read DB file");
    delete cur_page;
    return false;
  }
};


class TableBtree {
private:
  TableLeafPage *cur_page;  // TODO: Might conflict to page cache
  Pgsz cur_cell;            // TODO: cur_page(pgno, materialized by page cache) and cur_cell
                            // TODO: should treated as cursor
  //[IMPORTANT] TODO: Use cur_page and cur_cell as a cache (it has tremendous effects)

  FILE *f_db;
  DbHeader db_header;

  public:
  TableBtree(FILE *f_db)
    : cur_page(NULL), cur_cell(0), f_db(f_db), db_header(f_db)
  {
    assert(MYSQLITE_OK == db_header.read());
  }
  public:
  ~TableBtree()
  {}

  private:
  TableBtree();

  /*
  ** Retrieve a cell in order recursively
  **
  ** @param cellpos->pgno  First page to visit in this call
  ** @param cellpos->visit_path  It suggests next page to visit
  ** @param cellpos->cpa_idx  It suggests which cell to be retrieved.
  **   If the index exceeds num of cells in pgno, jump to next page.
  ** @param cellpos->cursor_end  true means end of traversal
  ** @param at_leaf  true means pgno is table leaf page
  */
  private:
  bool get_cellpos_fullscan_aux(FILE *f_db,
                                /* inout */
                                CellPos *cellpos)
  {
    assert(!cellpos->cursor_end);

    BtreePage *cur_page = new BtreePage(f_db, &db_header,
                                        cellpos->visit_path.back().pgno);
    assert(MYSQLITE_OK == cur_page->read());  // TODO: Cache

    btree_page_type btree_type = cur_page->get_btree_type();
    assert(btree_type == TABLE_LEAF || btree_type == TABLE_INTERIOR);

    // If at interior node, then to leftmost leaf
    if (btree_type == TABLE_INTERIOR) {
      TableInteriorPage *cur_interior_page = static_cast<TableInteriorPage *>(cur_page);
      vector<BtreePathNode> path;
      assert(cur_interior_page->get_path_to_leftmost_leaf(&path));
      cellpos->visit_path.insert(
        cellpos->visit_path.end(),
        path.begin(), path.end()
      );

      delete cur_page;
      cur_page = new BtreePage(f_db, &db_header,
                               cellpos->visit_path.back().pgno);
    }
    assert(cur_page->get_btree_type() == TABLE_LEAF);

    // get a cell
    bool has_cell = cur_page->has_ith_cell(++cellpos->cpa_idx);
    if (has_cell) {
      // cur_page has cell to point at
      return true;
    }
    else if (!has_cell && cellpos->visit_path.size() == 1) {
      // leaf == root. And no more cell
      cellpos->cursor_end = true;
      return true;
    }
    else if (!has_cell && cellpos->visit_path.size() > 1) {
      // Jump to parent who has more children
      while (cellpos->visit_path.size() > 1) {
        BtreePathNode child_node = cellpos->visit_path.back();
        cellpos->visit_path.pop_back();
        BtreePathNode parent_node = cellpos->visit_path.back();

        TableInteriorPage cur_interior_page(f_db, &db_header, parent_node.pgno);
        if (cur_interior_page.has_ith_cell(child_node.sib_idx + 1)) {
          // parent has next child
          break;
        }
      }
      // go to leftmost leaf
      TableInteriorPage *cur_interior_page = new TableInteriorPage(f_db, &db_header,
                                                                   cellpos->visit_path.back().pgno);
      vector<BtreePathNode> path;
      assert(cur_interior_page->get_path_to_leftmost_leaf(&path));
      delete cur_interior_page;

      cellpos->visit_path.insert(
        cellpos->visit_path.end(),
        path.begin(), path.end()
      );

      return get_cellpos_fullscan_aux(f_db, cellpos);
    }
    else assert(0);
  }

  /*
  ** Retrieve a cell in order recursively
  */
  public:
  bool get_cellpos_fullscan(FILE *f_db,
                            /* inout */
                            CellPos *cellpos)
  {
    return get_cellpos_fullscan_aux(f_db, cellpos);
  }

  /*
  ** Find a record from key recursively
  */
  private:
  bool get_cellpos_by_key_aux(FILE *f_db,
                              Pgno pgno, u64 key,
                              /* out */
                              CellPos *cellpos)
  {
    BtreePage *cur_page = new BtreePage(f_db, &db_header, pgno);
    if (MYSQLITE_OK != cur_page->read()) {  // TODO: Cache
      log_msg("Failed to read DB file");
      delete cur_page;
      return false;
    }

    btree_page_type btree_type = cur_page->get_btree_type();
    assert(btree_type == TABLE_LEAF || btree_type == TABLE_INTERIOR);

    if (btree_type == TABLE_LEAF) {
      // find a cell with its rowid == key
      TableLeafPage *cur_leaf_page = static_cast<TableLeafPage *>(cur_page);
      int n_cell = cur_leaf_page->get_n_cell();
      for (int i = 0; i < n_cell; ++i) {
        u64 rowid = cur_leaf_page->get_ith_cell_rowid(i);

        if (key == rowid) {  // TODO: Is assuming key == rowid OK? No cluster index?
          // found a cell to return.
          cellpos->pgno = pgno;
          cellpos->cpa_idx = i;
          delete cur_page;
          return true;
        }
      }
      delete cur_page;
      return false;
    }
    else if (btree_type == TABLE_INTERIOR) {
      // jump to one of children nodes
      TableInteriorPage *cur_interior_page = static_cast<TableInteriorPage *>(cur_page);
      int n_cell = cur_interior_page->get_n_cell();
      u64 prev_rowid = 0;
      for (int i = 0; i < n_cell; ++i) {
        struct TableInteriorPageCell cell;
        cur_interior_page->get_ith_cell(i, &cell);
        if (prev_rowid < key && key <= cell.rowid) {
          // found next page to traverse
          delete cur_page;
          return get_cellpos_by_key_aux(f_db, cell.left_child_pgno, key,
                                        cellpos);
        }
      }
      // cell corresponding the key is in the rightmost child page
      delete cur_page;
      return get_cellpos_by_key_aux(f_db,
                                    cur_interior_page->get_rightmost_pg(), key,
                                    cellpos);
    }
    else abort();  // Asserted not to come here
  }
  public:
  bool get_cellpos_by_key(FILE *f_db, Pgno root_pgno, u64 key,
                          /* out */
                          CellPos *cellpos) {
    return get_cellpos_by_key_aux(f_db, root_pgno, key,
                                  cellpos);
  }
};


#endif /* _SQLITE_FORMAT_H_ */

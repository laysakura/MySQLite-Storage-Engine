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

#include <sql_plugin.h>

#include "mysqlite_types.h"
#include "utils.h"


struct BtreePathNode {
  Pgno pgno;
  Pgno child_idx_to_visit;  // 0-origin

  public:
  BtreePathNode(Pgno pgno, Pgno child_idx_to_visit)
    : pgno(pgno), child_idx_to_visit(child_idx_to_visit) {}
};


static inline bool has_sqlite3_signature(FILE * const f)
{
  char s[SQLITE3_SIGNATURE_SZ];
  if (MYSQLITE_OK != mysqlite_fread(s, 0, SQLITE3_SIGNATURE_SZ, f)) return false;
  return strcmp(s, SQLITE3_SIGNATURE) == 0;
}


static inline u64 stype2len(u64 stype) {
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
      sprintf(message, "Permission denied: Cannot open %s in read mode.\n", path);
      *res = MYSQLITE_PERMISSION_DENIED;
      return NULL;
    }
    if (!has_sqlite3_signature(f)) {
      sprintf(message, "Format error: %s does not seem SQLite3 database.\n", path);
      *res = MYSQLITE_CORRUPT_DB;
      return NULL;
    }
  } else {
    *is_existing_db = false;
    f = fopen(path, "w+");
    if (!f) {
      sprintf(message, "Permission denied: Cannot create %s.\n", path);
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
    abort();  // TODO: Opening (creating) new SQLite DB is not yet supported.
    *res = MYSQLITE_DB_FILE_NOT_FOUND;
    return NULL;
  }

  return f_ret;
}


/*
** Database header functions.
**
** Real data is always on page cache(0).
*/
class DbHeader {
  public:
  static Pgsz get_pg_sz();

  public:
  static Pgsz get_reserved_space();

private:
  // Prohibit any way to create instance
  DbHeader();
  DbHeader(const DbHeader&);
  DbHeader& operator=(const DbHeader&);
};

/*
** Page class
*/
class Page {
public:
  u8 *pg_data;
  Pgno pgno;

  /*
  ** @note
  ** Constructor does not read(2) page contents.
  ** Call this->fetch() after object is constructed.
  */
  public:
  Page(Pgno pgno)
    : pg_data(NULL), pgno(pgno)
  {}
  public:
  virtual ~Page() {}

  public:
  errstat fetch();

  // Prohibit default constructor
 private:
  Page();
};

/*
** Btree page virtual class
*/
class BtreePage : public Page {
  public:
  BtreePage(Pgno pgno)
    : Page(pgno)
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
    if (pgno == 1) cpa_start += static_cast<Pgsz>(DB_HEADER_SZ);
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
  errstat fetch() {
    errstat res = Page::fetch();
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
  vector<u64> cols_offset;    // Can be longer than Pgsz (overflow page)
  vector<u64> cols_len;
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
    u64 offset = 0;
    u8 len;
    u64 hdr_sz = varint2u64(&data[offset], &len);
    offset += len;

    // Read column headers
    for (u64 read_hdr_sz = len; read_hdr_sz < hdr_sz; ) {
      u64 stype = varint2u64(&data[offset], &len);
      offset += len;
      read_hdr_sz += len;

      if (stype <= 9) {
        cols_type.push_back(static_cast<sqlite_type>(stype));
      } else if (stype >= 12) {
        cols_type.push_back(stype % 2 == 0 ? ST_BLOB : ST_TEXT);
      } else {
        log_msg("Invalid sqlite type (stype=%llu)\n", stype);
        return false;
      }
      cols_len.push_back(stype2len(stype));
    }

    // Read column bodies
    for (vector<u64>::iterator it = cols_len.begin();
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
  Pgno overflow_pgno;  // First overflow page num. 0 when cell has no overflow page.

  public:
  inline bool has_overflow_pg() const { return overflow_pgno != 0; }
};

class TableLeafPage : public BtreePage {
  public:
  TableLeafPage(Pgno pgno)
   : BtreePage(pgno)
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
    Pgsz usable_sz = DbHeader::get_pg_sz() - DbHeader::get_reserved_space();
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
    u64 offset = 0;
    memcpy(&buf_overflown_payload[offset], cell->payload.data, cell->payload_sz_in_origpg);
    offset += cell->payload_sz_in_origpg;
    Pgsz usable_sz = DbHeader::get_pg_sz() - DbHeader::get_reserved_space();
    u64 payload_sz_rem = cell->payload_sz - cell->payload_sz_in_origpg;

    for (Pgno overflow_pgno = cell->overflow_pgno; overflow_pgno != 0; ) {
      Page ovpg(overflow_pgno);
      assert(MYSQLITE_OK == ovpg.fetch());
      overflow_pgno = u8s_to_val<Pgno>(&ovpg.pg_data[0], sizeof(Pgno));
      Pgsz payload_sz_inpg = min<u64>(usable_sz - sizeof(Pgno), payload_sz_rem);
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
  TableInteriorPage(Pgno pgno)
   : BtreePage(pgno)
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
};


#endif /* _SQLITE_FORMAT_H_ */

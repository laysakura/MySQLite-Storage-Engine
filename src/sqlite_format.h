#ifndef _SQLITE_FORMAT_H_
#define _SQLITE_FORMAT_H_


#include "utils.h"


/*
** Constants
*/
#define SQLITE3_SIGNATURE_SZ 16
#define SQLITE3_SIGNATURE "SQLite format 3"

#define DB_HEADER_SZ 100

#define DBHDR_PGSZ_OFFSET 16
#define DBHDR_PGSZ_LEN 2

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

#define SQLITE3_VARINT_MAXLEN 9


/*
** Types
*/
typedef u32 Pgno;
typedef u16 Pgsz;

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


static inline bool has_sqlite3_signature(FILE * const f)
{
  char s[SQLITE3_SIGNATURE_SZ];
  if (!mysqlite_fread(s, 0, SQLITE3_SIGNATURE_SZ, f)) return false;
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
** pathをopenし，もし存在すればそれがSQLite DBとしてvalidなものかをチェックする
**
** @return
** NULL: Error. `message' is set.
*/
static inline FILE *open_sqlite_db(const char * const path,
                                   /* out */
                                   bool * const is_existing_db,
                                   char * const message)
{
  struct stat st;
  if (stat(path, &st) == 0) {
    *is_existing_db = true;
    FILE *f = fopen(path, "r");
    if (!f) {
      sprintf(message, "Permission denied: Cannot open %s in read mode.", path);
      return NULL;
    }
    if (!has_sqlite3_signature(f)) {
      sprintf(message, "Format error: %s does not seem SQLite3 database.", path);
      return NULL;
    }
    return f;
  } else {
    *is_existing_db = false;
    FILE *f = fopen(path, "w+");
    if (!f) {
      sprintf(message, "Permission denied: Cannot create %s.", path);
      return NULL;
    }
    return f;
  }
}
static inline FILE *open_sqlite_db(const char * const path,
                                   /* out */
                                   bool * const is_existing_db)
{
  char msg[1024];
  FILE *f_ret = open_sqlite_db(path, is_existing_db, msg);
  if (!f_ret) log("%s", msg);
  return f_ret;
}
static inline FILE *open_sqlite_db(const char * const existing_path)
{
  bool is_existing_db;
  FILE *f_ret = open_sqlite_db(existing_path, &is_existing_db);
  if (!f_ret) return NULL;

  if (!is_existing_db) {
    if (0 != fclose(f_ret)) perror("fclose() failed\n");
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

public:
  /*
  ** @note
  ** Constructor does not read(2) page contents.
  ** Call this->init() after object is constructed.
  */
  DbHeader(FILE * const f_db)
    : f_db(f_db)
  {
    assert(f_db);
    assert(hdr_data = (u8 *)malloc(DB_HEADER_SZ));
  }
  ~DbHeader() {
    free(hdr_data);
  }

  bool read() const {
    return mysqlite_fread(hdr_data, 0, DB_HEADER_SZ, f_db);
  }

  Pgsz get_pg_sz() const {
    return u8s_to_val<Pgsz>(&hdr_data[DBHDR_PGSZ_OFFSET], DBHDR_PGSZ_LEN);
  }

private:
  // Prohibit default constructor
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
  Pgno pg_id;

public:
  /*
  ** @note
  ** Constructor does not read(2) page contents.
  ** Call this->init() after object is constructed.
  */
  Page(FILE * const f_db,
       const DbHeader * const db_header,
       Pgno pg_id)
    : f_db(f_db), db_header(db_header), pg_id(pg_id)
  {
    assert(f_db);
    assert(db_header);
    assert(pg_data = (u8 *)malloc(db_header->get_pg_sz()));
  }
  virtual ~Page() {
    free(pg_data);
  }

  bool read() const {
    assert(db_header);
    Pgsz pg_sz = db_header->get_pg_sz();
    return mysqlite_fread(pg_data, pg_sz * (pg_id - 1), pg_sz, f_db);
  }

private:
  // Prohibit default constructor
  Page() : f_db(NULL), db_header(NULL) {}
};

/*
** Btree page virtual class
*/
class BtreePage : public Page {
public:
  BtreePage(FILE * const f_db,
            const DbHeader * const db_header,
            Pgno pg_id)
    : Page(f_db, db_header, pg_id)
  {}

  // Btree header info

  btree_page_type get_btree_type() const {
    return (btree_page_type)pg_data[
      pg_id == 1 ? DB_HEADER_SZ + BTREEHDR_BTREETYPE_OFFSET :
                   BTREEHDR_BTREETYPE_OFFSET
    ];
  }

  Pgsz get_freeblock_offset() const {
    return u8s_to_val<Pgsz>(
      &pg_data[
        pg_id == 1 ? DB_HEADER_SZ + BTREEHDR_FREEBLOCKOFST_OFFSET :
                     BTREEHDR_FREEBLOCKOFST_OFFSET
      ],
      BTREEHDR_FREEBLOCKOFST_LEN);
  }

  Pgsz get_n_cell() const {
    return u8s_to_val<Pgsz>(
      &pg_data[
        pg_id == 1 ? DB_HEADER_SZ + BTREEHDR_NCELL_OFFSET :
                     BTREEHDR_NCELL_OFFSET
      ],
      BTREEHDR_NCELL_LEN);
  }

  Pgsz get_cell_content_area_offset() const {
    return u8s_to_val<Pgsz>(
      &pg_data[
        pg_id == 1 ? DB_HEADER_SZ + BTREEHDR_CELLCONTENTAREAOFST_OFFSET :
                     BTREEHDR_CELLCONTENTAREAOFST_OFFSET
      ],
      BTREEHDR_CELLCONTENTAREAOFST_LEN);
  }

  u8 get_n_fragmentation() const {
    return pg_data[
      pg_id == 1 ? DB_HEADER_SZ + BTREEHDR_NFRAGMENTATION_OFFSET :
                   BTREEHDR_NFRAGMENTATION_OFFSET
    ];
  }

  Pgno get_rightmost_pg() const {
    assert(get_btree_type() == INDEX_INTERIOR ||
           get_btree_type() == TABLE_INTERIOR);
    return u8s_to_val<Pgno>(
      &pg_data[
        pg_id == 1 ? DB_HEADER_SZ + BTREEHDR_RIGHTMOSTPG_OFFSET :
                     BTREEHDR_RIGHTMOSTPG_OFFSET
      ],
      BTREEHDR_RIGHTMOSTPG_LEN);
  }

protected:
  bool is_valid_hdr() const {
    bool is_valid = true;
    Pgsz pg_sz = db_header->get_pg_sz();
    {
      btree_page_type type = get_btree_type();
      is_valid &= (type == INDEX_INTERIOR ||
                   type == TABLE_INTERIOR ||
                   type == INDEX_LEAF ||
                   type == TABLE_LEAF);
    }
    {
      Pgsz offset = get_freeblock_offset();
      is_valid &= (0 <= offset && offset < pg_sz);
    }
    {
      Pgsz n_cell = get_n_cell();
      is_valid &= (0 <= n_cell && n_cell < pg_sz);
    }
    {
      Pgsz offset = get_cell_content_area_offset();
      is_valid &= (0 <= offset && offset <= pg_sz);
      // cell_content_area_offset == pg_sz means
      // there are no cells yet
    }
    {
      Pgsz n_fragmentation = get_n_fragmentation();
      is_valid &= (0 <= n_fragmentation && n_fragmentation < pg_sz);
    }
    return is_valid;
  }
public:

  // Cell info
protected:
  /*
  ** @param i  0-origin index.
  **
  ** @return  0 if i is out of larger than the actual number of cells.
  */
  Pgsz get_ith_cell_offset(Pgsz i) const {
    if (i >= get_n_cell()) return 0;

    // cpa stands for Cell Pointer Array
    btree_page_type type = get_btree_type();
    Pgsz cpa_start = (type == INDEX_LEAF || type == TABLE_LEAF) ?
      BTREEHDR_SZ_LEAF : BTREEHDR_SZ_INTERIOR;
    if (pg_id == 1) cpa_start += DB_HEADER_SZ;
    return u8s_to_val<Pgsz>(&pg_data[cpa_start + CPA_ELEM_LEN * i], CPA_ELEM_LEN);
  }
public:

  Pgno get_leftchild_pgno(Pgsz start_offset) const {
    return u8s_to_val<Pgno>(&pg_data[start_offset], BTREECELL_LECTCHILD_LEN);
  }

  /*
  ** @return Payload sz. This can be longer than page sz
  **   when the cell has overflow pages.
  */
  u64 get_payload_sz(Pgsz start_offset, u8 *len) const {
    return varint2u64(&pg_data[start_offset], len);
  }

  u64 get_rowid(Pgsz start_offset, u8 *len) const {
    return varint2u64(&pg_data[start_offset], len);
  }

  Pgno get_overflow_pgno(Pgsz start_offset) const;

  // Page management
  bool read() const {
    return Page::read() && is_valid_hdr();
  }

};

/*
** Table leaf page
*/
class TableLeafPage : public BtreePage {
public:
  TableLeafPage(FILE * const f_db,
                const DbHeader * const db_header,
                Pgno pg_id)
   : BtreePage(f_db, db_header, pg_id)
  {}

  /*
  ** TODO: maybe reading overflow_pgno, overflown_payload_sz has bugs
  **
  ** @param i  Specifies i-th cell in the page (0-origin).
  ** @param overflow_pgno  Pgno to overflow page. 0 if no overflow page.
  ** @param overflown_payload_sz  Remaining payload size.
  **   It can be larger than page size when 2 or more overflow page are involved.
  **
  ** @return false on error
  */
  public:
  bool get_icell_cols(Pgsz i,
                      /*out*/
                      u64 *rowid,
                      Pgno *overflow_pgno, u64 *overflown_payload_sz,
                      vector<Pgsz> &cols_offset,
                      vector<Pgsz> &cols_len,
                      vector<sqlite_type> &cols_type) const
  {
    u8 len;
    Pgsz offset = get_ith_cell_offset(i);

    u64 payload_sz = get_payload_sz(offset, &len);
    offset += len;

    *rowid = get_rowid(offset, &len);
    offset += len;

    { // Read payload
      // @see
      // Extracting SQLite records - Figure 4. SQLite record format
      Pgsz payload_start_offset = offset;
      u64 hdr_sz = varint2u64(&pg_data[offset], &len);
      offset += len;

      // Read column headers
      for (u64 read_hdr_sz = len; read_hdr_sz < hdr_sz; ) {
        Pgsz stype = varint2u64(&pg_data[offset], &len);
        offset += len;
        read_hdr_sz += len;

        if (stype <= 9) {
          cols_type.push_back(static_cast<sqlite_type>(stype));
        } else if (stype >= 12) {
          cols_type.push_back(stype % 2 == 0 ? ST_BLOB : ST_TEXT);
        } else {
          log("Invalid sqlite type (stype=%d) on page#%u cell#%u\n",
              stype, pg_id, i);
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

      *overflown_payload_sz = payload_sz - (offset - payload_start_offset);
    }

    // Read overflow pgno
    *overflow_pgno = u8s_to_val<Pgno>(&pg_data[offset], BTREECELL_OVERFLOWPGNO_LEN);
    return true;
  }
};


#endif /* _SQLITE_FORMAT_H_ */

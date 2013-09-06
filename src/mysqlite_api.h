#ifndef _MYSQLITE_API_H_
#define _MYSQLITE_API_H_


#include "mysqlite_types.h"
#include "sqlite_format.h"

namespace mysqlite {

/***********************************************************************
** Classes
***********************************************************************/

/*
** Used to iterate table cursor.
** Used by both fullscan and index scan.
*/
class RowCursor {
protected:
  vector<BtreePathNode> visit_path;  // Save the history of traversal.
               // Example:
               //
               // 0-+-0
               //   |
               //   +-1--+-0
               //   |    |
               //   +-2  +-1
  Pgsz cpa_idx;    // Cell Pointer Array index
  bool prev_overflown;  // Used for determine whether to delete rec_buf.
  u8 *rec_buf;  // Buffer for pointing or copying record data in page cache.
                // rec_buf is set on every next() call.
                // Copy is only necessary when overflow page exists.
  RecordCell cell;     // Metadata for cell

  /*
  ** Whether to have remnant rows
  */
  public:
  virtual bool next() = 0;

  /*
  ** Cell value getter.
  */
  public:
  mysqlite_type get_type(int colno) const;
  public:
  int get_int(int colno) const;
  public:
  void get_blob(int colno,
                /* out */
                vector<u8> &buf) const;
  public:
  string get_raw_string(int colno) const;

  public:
  virtual ~RowCursor();

  protected:
  RowCursor(Pgno root_pgno);

};

/*
** TODO: Move this class to other file so that user cannot see it.
** Users do not directly use this class.
**
** Used to iterate table cursor in fullscan.
*/
class FullscanCursor : public RowCursor {

  /*
  ** @param f_db  Already opened file descriptor.
  **   Used to open B-tree pages.
  */
  public:
  FullscanCursor(Pgno root_pgno);
  public:
  ~FullscanCursor();

  public:
  bool next();

  private:
  void fill_rec_data();

  private:
  bool jump_to_parent_or_finish_traversal();
};


/*
** Open a connection to a database
*/
class Connection {
private:
  unsigned int refcnt_rdlock_db;
  FILE *f_db;   // TODO: handler socket とかからMAIIなファイルオブジェクトパクる

  public:
  Connection()
    : refcnt_rdlock_db(0),
      f_db(NULL)
  {}
  public:
  ~Connection();

  /*
  ** Open a connection to a db
  */
  public:
  errstat open(const char * const db_path);

  public:
  bool is_opened() const;

  /*
  ** Fullscan table.
  ** retval must be deleted (good to use std::unique_ptr)
  */
  public:
  RowCursor *table_fullscan(const char * const table);
  private:
  RowCursor *table_fullscan(Pgno tbl_root);

  /*
    Read lock to SQLite DB file.
    Thread safe functions.

    @returns HA_*
   */
  public:
  int rdlock_db();

  /*
    Unlock to SQLite DB file.
    Thread safe functions.

    @returns HA_*
   */
  public:
  int unlock_db();
};


}


#endif /* _MYSQLITE_API_H_ */

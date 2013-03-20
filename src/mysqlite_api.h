#ifndef _MYSQLITE_API_H_
#define _MYSQLITE_API_H_


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
  FILE *f_db;

  vector<BtreePathNode> visit_path;  // Save the history of traversal.
               // Example:
               //
               // 0-+-0
               //   |
               //   +-1-+-0
               //   |   |
               //   +-2 +-1
  Pgsz cpa_idx;    // Cell Pointer Array index

  /*
  ** Whether to have remnant rows
  */
  public:
  virtual bool next() = 0;

  /*
  ** Close cursor
  */
  public:
  virtual void close() = 0;

  /*
  ** Cell value getter.
  */
  public:
  mysqlite_type get_type(int colno) const;
  public:
  int get_int(int colno) const;
  public:
  const char *get_text(int colno) const;  // TODO: how to prevent memory leak? Self mem mngmt?

  public:
  virtual ~RowCursor() {}

  protected:
  RowCursor(FILE *f_db, Pgno root_pgno);

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
  FullscanCursor(FILE *f_db, Pgno root_pgno);

  public:
  void close();

  public:
  bool next();

  public:
  virtual ~FullscanCursor();

  private:
  bool jump_to_parent_or_finish_traversal();
};


/*
** Open a connection to a database
*/
class Connection {
private:
  FILE *f_db;

  public:
  Connection() : f_db(NULL) {}

  /*
  ** Open a connection to a db
  **
  ** @note
  ** Connection::close() must be used to the returned instance.
  */
  public:
  errstat open(const char * const db_path);

  public:
  bool is_opened() const;

  /*
  ** Close connection
  */
  public:
  void close();

  /*
  ** Fullscan table.
  ** retval must call RowCursor::close()
  */
  public:
  RowCursor *table_fullscan(const char * const table);
  private:
  RowCursor *table_fullscan(Pgno tbl_root);
};


/***********************************************************************
** Functions
***********************************************************************/

}


#endif /* _MYSQLITE_API_H_ */

#include "pcache.h"
#include "sqlite_format.h"


/***********************************************************************
 ** PageCache class
 ***********************************************************************/
errstat PageCache::refresh(const char * const db_path)
{
  errstat res;

  if (f_db) {
    fclose(f_db);
    f_db = NULL;
  }
  f_db = open_sqlite_db(db_path, &res);

  if (res == MYSQLITE_OK) {
    // DB file exists on db_path

    // Check page size of db_path.
    // Note that pcache(0) does not have page#1 data yet.
    // Directly read DB file.
    u8 pgsz_data[DBHDR_PGSZ_LEN];
    mysqlite_fread(pgsz_data, DBHDR_PGSZ_OFFSET, DBHDR_PGSZ_LEN, f_db);
    pgsz = u8s_to_val<Pgsz>(pgsz_data, DBHDR_PGSZ_LEN);
  } else if (res == MYSQLITE_DB_FILE_NOT_FOUND) {
    // Newly creating database file

    // Decide page size
    // TODO: how to change page size?
    pgsz = PAGE_MIN_SZ;
  } else {
    // error
    return res;
  }

  // 0-fill memory
  n_pg = (pcache_sz / pgsz) + 1;
  memset(the_cache, 0, pcache_sz + pcache_idx_sz());

  // Put DB page#1 onto pcache(0)
  pcache_idx[0] = SQLITE_MASTER_ROOTPGNO;
  mysqlite_fread(&the_cache[0], 0, pgsz, f_db);

  return res;
}

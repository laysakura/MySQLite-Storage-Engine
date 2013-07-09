#include "pcache.h"
#include "sqlite_format.h"


/***********************************************************************
 ** PageCache class
 ***********************************************************************/
errstat PageCache::refresh(FILE *f_db)  // TODO: MAIIなファイルオブジェクト
{
  if (this->f_db) {
    fclose(this->f_db);
  }
  this->f_db = f_db;

  errstat res = MYSQLITE_OK;

  // Check page size of db_path.
  // Note that pcache(0) does not have page#1 data yet.
  // Directly read DB file.
  u8 pgsz_data[DBHDR_PGSZ_LEN];
  mysqlite_fread(pgsz_data, DBHDR_PGSZ_OFFSET, DBHDR_PGSZ_LEN, f_db);
  pgsz = u8s_to_val<Pgsz>(pgsz_data, DBHDR_PGSZ_LEN);

  // 0-fill memory
  n_pg = (pcache_sz / pgsz) + 1;
  memset(the_cache, 0, pcache_sz + pcache_idx_sz());

  // Put DB page#1 onto pcache(0)
  pcache_idx[0] = SQLITE_MASTER_ROOTPGNO;
  mysqlite_fread(&the_cache[0], 0, pgsz, f_db);

  return res;
}

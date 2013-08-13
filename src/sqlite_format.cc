#include "sqlite_format.h"
#include "pcache_mmap.h"


/***********************************************************************
** DbHeader class
***********************************************************************/
Pgsz DbHeader::get_pg_sz()
{
  PageCache *pcache = PageCache::get_instance();
  assert(pcache->is_rd_locked());
  u8 *hdr_data = pcache->fetch(SQLITE_MASTER_ROOTPGNO);
  return u8s_to_val<Pgsz>(&hdr_data[DBHDR_PGSZ_OFFSET], DBHDR_PGSZ_LEN);
}

Pgsz DbHeader::get_reserved_space()
{
  PageCache *pcache = PageCache::get_instance();
  assert(pcache->is_rd_locked());
  u8 *hdr_data = pcache->fetch(SQLITE_MASTER_ROOTPGNO);
  return u8s_to_val<Pgsz>(&hdr_data[DBHDR_RESERVEDSPACE_OFFSET], DBHDR_RESERVEDSPACE_LEN);
}

u32 DbHeader::get_file_change_counter()
{
  PageCache *pcache = PageCache::get_instance();
  assert(pcache->is_rd_locked());
  u8 *hdr_data = pcache->fetch(SQLITE_MASTER_ROOTPGNO);
  return u8s_to_val<Pgsz>(&hdr_data[DBHDR_FCC_OFFSET], DBHDR_FCC_LEN);
}

errstat DbHeader::inc_file_change_counter()
{
  PageCache *pcache = PageCache::get_instance();
  if (!pcache->is_wr_locked()) return MYSQLITE_FLOCK_NEEDED;
  u8 *hdr_data = pcache->fetch(SQLITE_MASTER_ROOTPGNO);
  u32 fcc = u8s_to_val<Pgsz>(&hdr_data[DBHDR_FCC_OFFSET], DBHDR_FCC_LEN);
  *(&hdr_data[DBHDR_FCC_OFFSET]) = fcc + 1;
  return MYSQLITE_OK;
}

/***********************************************************************
** Page class
***********************************************************************/
errstat Page::fetch()
{
  PageCache *pcache = PageCache::get_instance();
  pg_data = pcache->fetch(pgno);
  return MYSQLITE_OK;  // TODO: page cache should return status
}

#include <sys/mman.h>
#include <fcntl.h>

#include "pcache.h"


/***********************************************************************
 ** PageCache class
 ***********************************************************************/
PageCache::PageCache()
  : sqlite_db(), p_mapped(NULL), lock_state(UNLOCKED), n_reader(0), pgsz(0)
{
}

PageCache::~PageCache()
{
}

errstat PageCache::open(const char * const path)
{
  std::lock_guard<std::mutex> lock(mutex);

  assert(!is_opened());  // TODO: support 2 or more DB open
  sqlite_db.reset(new SqliteDb(path));

  if (sqlite_db->mode() == SqliteDb::FAIL) {
    return MYSQLITE_CANNOT_OPEN_DB_FILE;
  }

  // TODO: DB file can be updated.
  // Needs larger memory than file size?
  p_mapped = (u8 *)mmap(0, sqlite_db->file_size(), PROT_READ | PROT_WRITE, MAP_SHARED, sqlite_db->fd(), 0);

  // Check page size of db_path.
  pgsz = u8s_to_val<Pgsz>(&p_mapped[DBHDR_PGSZ_OFFSET], DBHDR_PGSZ_LEN);

  return MYSQLITE_OK;
}

void PageCache::close()
{
  std::lock_guard<std::mutex> lock(mutex);

  assert(is_opened());
  munmap(p_mapped, sqlite_db->file_size());
  p_mapped = NULL;
  sqlite_db.reset();  // TODO: そもそもこんなの書かないで済むようにするためのRAII．
                     // pcache自体がRAIIじゃないとうまみがない
}

bool PageCache::is_opened() const
{
  return sqlite_db && sqlite_db->mode() != SqliteDb::FAIL;
}

u8 * PageCache::fetch(Pgno pgno) const
{
  my_assert(pgno >= 1);
  my_assert(is_rd_locked());
  return &p_mapped[pgsz * (pgno - 1)];
}

/**
 * @TODO  Shares flock with other threads using reference counter?
 */
void PageCache::rd_lock()
{
  struct flock flock;
  flock.l_whence = SEEK_SET;
  flock.l_start = 0;
  flock.l_len = 0;
  flock.l_type = F_RDLCK;

  std::lock_guard<std::mutex> lock(mutex);
  fcntl(sqlite_db->fd(), F_SETLKW, &flock);
  lock_state = RD_LOCKED;
  ++n_reader;
}

void PageCache::upgrade_lock()
{
  assert(is_rd_locked());
  struct flock flock;
  flock.l_whence = SEEK_SET;
  flock.l_start = 0;
  flock.l_len = 0;
  flock.l_type = F_RDLCK;

  std::lock_guard<std::mutex> lock(mutex);
  fcntl(sqlite_db->fd(), F_SETLKW, &flock);
  lock_state = WR_LOCKED;
}

void PageCache::unlock()
{
  assert(is_rd_locked() || is_wr_locked());
  struct flock flock;
  flock.l_whence = SEEK_SET;
  flock.l_start = 0;
  flock.l_len = 0;
  flock.l_type = F_UNLCK;

  std::lock_guard<std::mutex> lock(mutex);
  fcntl(sqlite_db->fd(), F_SETLKW, &flock);
  --n_reader;
  if (n_reader == 0) lock_state = UNLOCKED;
}

bool PageCache::is_rd_locked() const
{
  return lock_state == PageCache::RD_LOCKED;
}

bool PageCache::is_wr_locked() const
{
  return lock_state == PageCache::WR_LOCKED;
}

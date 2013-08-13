#include <sys/mman.h>
#include <fcntl.h>

#include "pcache_mmap.h"


/***********************************************************************
 ** PageCache class
 ***********************************************************************/
PageCache::PageCache()
  : p_db(NULL), sqlite_db(), lock_state(UNLOCKED), n_reader(0), mmap_size(0), pgsz(0)
{
  pthread_mutex_init(&mutex, NULL);
}

PageCache::~PageCache()
{
  pthread_mutex_destroy(&mutex);
}

errstat PageCache::open(const char * const path)
{
  pthread_mutex_lock(&mutex);

  assert(!is_opened());  // TODO: support 2 or more DB open
  sqlite_db.reset(path);

  if (sqlite_db->mode() == SqliteDb::FAIL) {
    pthread_mutex_unlock(&mutex);
    return res;
  }

  // TODO: DB file can be updated.
  // Needs larger memory than file size?
  p_db = (u8 *)mmap(0, sqlite_db->file_size(), PROT_READ | PROT_WRITE, MAP_SHARED, sqlite_db->fd(), 0);

  // Check page size of db_path.
  pgsz = u8s_to_val<Pgsz>(&p_db[DBHDR_PGSZ_OFFSET], DBHDR_PGSZ_LEN);

  pthread_mutex_unlock(&mutex);
  return MYSQLITE_OK;
}

void PageCache::close()
{
  pthread_mutex_lock(&mutex);

  assert(is_opened());
  munmap(p_db, sqlite_db->file_size());
  ::close(fd_db);
  p_db = NULL;

  pthread_mutex_unlock(&mutex);
}

bool PageCache::is_opened() const
{
  return sqlite_db && sqlite_db->mode() != SqliteDb::FAIL;
}

u8 * PageCache::fetch(Pgno pgno) const
{
  my_assert(pgno >= 1);
  return &p_db[pgsz * (pgno - 1)];
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

  pthread_mutex_lock(&mutex);
  fcntl(fd_db, F_SETLKW, &flock);
  lock_state = RD_LOCKED;
  ++n_reader;
  pthread_mutex_unlock(&mutex);
}

void PageCache::upgrade_lock()
{
  assert(is_rd_locked());
  struct flock flock;
  flock.l_whence = SEEK_SET;
  flock.l_start = 0;
  flock.l_len = 0;
  flock.l_type = F_RDLCK;

  pthread_mutex_lock(&mutex);
  fcntl(fd_db, F_SETLKW, &flock);
  lock_state = WR_LOCKED;
  pthread_mutex_unlock(&mutex);
}

void PageCache::unlock()
{
  assert(is_rd_locked() || is_wr_locked());
  struct flock flock;
  flock.l_whence = SEEK_SET;
  flock.l_start = 0;
  flock.l_len = 0;
  flock.l_type = F_UNLCK;

  pthread_mutex_lock(&mutex);
  fcntl(fd_db, F_SETLKW, &flock);
  --n_reader;
  if (n_reader == 0) lock_state = UNLOCKED;
  pthread_mutex_unlock(&mutex);
}

bool PageCache::is_rd_locked() const
{
  return lock_state == PageCache::RD_LOCKED;
}

bool PageCache::is_wr_locked() const
{
  return lock_state == PageCache::WR_LOCKED;
}

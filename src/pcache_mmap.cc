#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>

#include "pcache_mmap.h"
#include "sqlite_format.h"


/***********************************************************************
 ** PageCache class
 ***********************************************************************/
PageCache::PageCache()
  : p_db(NULL), fd_db(0), lock_state(UNLOCKED), mmap_size(0), pgsz(0)
{
}

errstat PageCache::open(const char * const path)
{
  if ((fd_db = ::open(path, O_RDWR | O_CREAT)) == -1) {
    perror("open");
    log_errstat(MYSQLITE_DB_FILE_NOT_FOUND);
    return MYSQLITE_DB_FILE_NOT_FOUND;
  }

  // getting file size
  struct stat stbuf;
  if (fstat(fd_db, &stbuf) == -1) {
    perror("fstat");
    log_errstat(MYSQLITE_DB_FILE_NOT_FOUND);
    return MYSQLITE_DB_FILE_NOT_FOUND;
  }
  mmap_size = stbuf.st_size;

  // TODO: DB file can be updated.
  // Needs larger memory than file size?
  p_db = (u8 *)mmap(0, mmap_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_db, 0);

  // Check page size of db_path.
  pgsz = u8s_to_val<Pgsz>(&p_db[DBHDR_PGSZ_OFFSET], DBHDR_PGSZ_LEN);

  return MYSQLITE_OK;
}

void PageCache::close()
{
  munmap(p_db, mmap_size);
  ::close(fd_db);
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
  fcntl(fd_db, F_SETLKW, &flock);
  lock_state = RD_LOCKED;
}

void PageCache::upgrade_lock()
{
  assert(is_rd_locked());
  struct flock flock;
  flock.l_whence = SEEK_SET;
  flock.l_start = 0;
  flock.l_len = 0;
  flock.l_type = F_RDLCK;
  fcntl(fd_db, F_SETLKW, &flock);
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
  fcntl(fd_db, F_SETLKW, &flock);
  lock_state = UNLOCKED;
}

bool PageCache::is_rd_locked() const
{
  return lock_state == PageCache::RD_LOCKED;
}

bool PageCache::is_wr_locked() const
{
  return lock_state == PageCache::WR_LOCKED;
}

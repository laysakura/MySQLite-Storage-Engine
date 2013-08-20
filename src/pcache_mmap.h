#ifndef _PCACHE_MMAP_H_
#define _PCACHE_MMAP_H_


#if (__GNUC__ >= 4 && __GNUC_MINOR__ >= 5)  // See: http://www.mail-archive.com/gcc-bugs@gcc.gnu.org/msg270025.html
#include <memory>
#else // (gcc < 4.5)
#include <bits/unique_ptr.h>
#endif

#include <mutex>

#include "mysqlite_types.h"
#include "utils.h"


/**
 * PageCache by mmap
 */
class PageCache {
private:
  std::unique_ptr<SqliteDb> sqlite_db;
  u8 *p_mapped;
  enum {
    UNLOCKED,
    RD_LOCKED,
    WR_LOCKED,
  } lock_state;
  int n_reader;  // reference counter for read locks
  Pgsz pgsz;
  std::mutex mutex;

  public:
  static PageCache *get_instance() {
    static PageCache instance;
    return &instance;
  }

  /**
   * Initialization
   *
   * Called when new SQLite DB is attached
   */
  public:
  errstat open(const char * const path);
  void close();
  bool is_opened() const;

  /**
   * Fetch a page.
   * Locks must be held before reading/writing to returned pointer.
   * This function does not check the lock state since caller can freely
   * use the returned pointer at any time after this call.
   *
   * @return pointer to mmaped-page.
   */
  public:
  u8 *fetch(Pgno pgno) const;

  /**
   * Locks
   */
  public:
  void rd_lock();
  void upgrade_lock();  // rd_lock -> wr_lock
  void unlock();
  bool is_rd_locked() const;
  bool is_wr_locked() const;

  public:
  PageCache();
  ~PageCache();

  private:
  PageCache(const PageCache&);
  PageCache& operator=(const PageCache&);
};


#endif /* _PCACHE_MMAP_H_ */

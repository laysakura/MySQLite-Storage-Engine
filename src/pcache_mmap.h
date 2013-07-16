#ifndef _PCACHE_MMAP_H_
#define _PCACHE_MMAP_H_


#include "mysqlite_types.h"
#include "utils.h"


/**
 * PageCache by mmap
 */
class PageCache {
private:
  u8 *p_db;
  int fd_db;
  enum {
    UNLOCKED,
    RD_LOCKED,
    WR_LOCKED,
  } lock_state;
  size_t mmap_size;
  Pgsz pgsz;

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

  private:
  PageCache(const PageCache&);
  PageCache& operator=(const PageCache&);
};


#endif /* _PCACHE_MMAP_H_ */

#ifndef _PCACHE_H_
#define _PCACHE_H_


#include "mysqlite_types.h"
#include "utils.h"


#define PCACHE_MIN_SZ PAGE_MAX_SZ


/**
 * PageCache
 *
 * Singleton
 */
class PageCache {
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
  errstat refresh(FILE *f_db);

  /**
   * Fetch page
   *
   * If the specified page is on the page cache, it is returned.
   * Otherwise, the page is read from DB file onto page cache and it is returned.
   */
  public:
  u8 *fetch(Pgno pgno) const;

  /**
   * Whether page cache is ready to use
   */
  public:
  bool is_ready() const;

  private:
  // Prohibit any way to create instance from outsiders
  PageCache()
   : pcache_sz(0), n_pg(0), pgsz(0),
    the_cache(NULL), pcache_idx(NULL), f_db(NULL)
    {}
  PageCache(const PageCache&);
  PageCache& operator=(const PageCache&);
};


#endif /* _PCACHE_H_ */

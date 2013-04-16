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
private:
  u64 pcache_sz;  // unique to a mysqld process
  Pgno n_pg;      // (pcache_sz / pgsze) + 1
  Pgsz pgsz;      // decided by each SQLite DB file
  u8 *the_cache;  // unique to a mysqld process
  Pgno *pcache_idx;  // Uneque to a mysqld process.
                     // pcache_idx[i] == j means page#j of SQLite DB
                     // is on pcache(i).
                     // pcache_idx[0] == 1 always suffices.
                     // TODO: compression
  FILE *f_db;

  /**
   * The interface to get instance
   */
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
  errstat refresh(const char * const db_path);

  /**
   * Allocate heap
   *
   * Called when mysqld and MySQLite have started.
   */
  public:
  void alloc(u64 pcache_sz) {
    assert(PCACHE_MIN_SZ <= pcache_sz);
    this->pcache_sz = pcache_sz;
    the_cache = new u8[pcache_sz + pcache_idx_sz()]; // TODO: more sophisticated mem allocation
                                                     // (see InnoDB's ut_malloc())
    pcache_idx = (Pgno *)(the_cache + pcache_sz);
  }

  /**
   * Free heap
   *
   * Called when MySQLite and mysqld end.
   */
  public:
  void free() {
    if (f_db) {
      fclose(f_db);
      f_db = NULL;
    }
    delete the_cache;
  }

  /**
   * The number of elements of the_cache is "(pcache_sz / 512) + 1"
   * since 512 is the least page size of SQLite.
   * Therefore, len(pcache_idx) >= n_pg always suffices.
   */
  private:
  u64 pcache_idx_sz() const;

  /**
   * Fetch page
   *
   * If the specified page is on the page cache, it is returned.
   * Otherwise, the page is read from DB file onto page cache and it is returned.
   */
  public:
  u8 *fetch(Pgno pgno) const {
    assert(pgno >= 1);
    assert(is_ready());

    Pgno pcno = pgno_to_pcacheno(pgno);
    if (pgno == pcache_idx[pcno]) {
      // Page#pgno is already on cache
      return &the_cache[pcno * pgsz];
    }
    else {
      // Page#pgno should be read(2) from DB file
      // TODO: cache eviction
      assert(f_db);
      assert(MYSQLITE_OK == mysqlite_fread(&the_cache[pcno * pgsz], (pgno - 1) * pgsz, pgsz, f_db));
      return &the_cache[pcno * pgsz];
    }
  }

  /**
   * Whether page cache is ready to use
   */
 public:
  bool is_ready() const {
    return f_db != NULL;
  }

  /**
   * Hash function to map SQLite DB page number -> pcache number
   */
 private:
  Pgno pgno_to_pcacheno(Pgno pgno) const {
    if (pgno == 1) return 0;  // page#1 should be always on pcache
    return 1 + (pgno - 2) % n_pg;
  }

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
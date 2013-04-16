#ifndef _PCACHE_H_
#define _PCACHE_H_


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
  static PageCache get_instance() const {
    static PageCache instance;
    return &instance;
  }

  /**
   * Initialization
   *
   * Called when new SQLite DB is attached
   */
  public:
  void refresh(FILE * const new_f_db) {
    f_db = new_f_db;

    // Check page size of new_f_db
    DbHeader db_header(new_f_db);
    pgsz = new_f_db.get_pg_sz();
    n_pg = (pcache_sz / new_pgsz) + 1;

    // 0-fill memory
    memset(the_cache, 0, pcache_sz + pcache_idx_sz());

    // Put DB page#1 onto pcache(0)
    pcache_idx[0] = SQLITE_MASTER_ROOTPGNO;
    mysqlite_fread(&the_cache[0], 0, pgsz, f_db);
  }

  /**
   * Allocate heap
   *
   * Called when mysqld and MySQLite have started.
   */
  public:
  void alloc(u64 pcache_sz) {
    this->pcache_sz = pcache_sz;

    // The number of elements of the_cache is "(pcache_sz / 512) + 1"
    // since 512 is the least page size of SQLite.
    // Therefore, len(pcache_idx) >= n_pg always suffices.
    the_cache = new u8[pcache_sz + pcache_idx_sz()];
    pcache_idx = (Pgno *)(the_cache + pcache_sz);
  }

  /**
   * Free heap
   *
   * Called when MySQLite and mysqld end.
   */
  public:
  void free() {
    delte the_cache;
  }

  private:
  inline u64 pcache_idx_sz() const {
    return sizeof(Pgno) * ((pcache_sz / PAGE_MIN_SZ) + 1);
  }

  /**
   * Fetch page
   *
   * If the specified page is on the page cache, it is returned.
   * Otherwise, the page is read from DB file onto page cache and it is returned.
   */
  public:
  u8 *fetch(Pgno pgno) {
    assert(pgno >= 1);
    assert(pcache_idx[0] == SQLITE_MASTER_ROOTPGNO);  // Suffices after refresh() is called

    Pgno pcno = pgno_to_pcacheno(pgno);
    if (pgno == pcache_idx[pcno]) {
      // Page#pgno is already on cache
      return &the_cache[pcno * pgsz];
    }
    else {
      // Page#pgno should be read(2) from DB file
      // TODO: cache eviction
      assert(MYSQLITE_OK == mysqlite_fread(&the_cache[pcno * pgsz], pgno * pgsz, pgsz, f_db));
      return &the_cache[pcno * pgsz];
    }
  }

private:
  // Prohibit any way to create instance from outsiders
  PageCache()
    : pcache_sz(0), n_pg(0), pgsz(0),
      the_cache(NULL), pcache_idx(NULL), f_db(NULL)
  {};
  PageCache(const PageCache& obj);
  PageCache& oprator=(const PageCache& obj);
};


#endif /* _PCACHE_H_ */

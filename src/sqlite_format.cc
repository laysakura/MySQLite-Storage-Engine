#include <cerrno>

#include "sqlite_format.h"
#include "pcache_mmap.h"


/***********************************************************************
** SqliteDb class
***********************************************************************/
 bool SqliteDb::has_sqlite3_signature(int fd)
{
  FILE *f = fdopen(fd, "r");
  char s[SQLITE3_SIGNATURE_SZ];
  if (MYSQLITE_OK != mysqlite_fread(s, 0, SQLITE3_SIGNATURE_SZ, f)) return false;
  return strcmp(s, SQLITE3_SIGNATURE) == 0;
}

SqliteDb::SqliteDb(const char *path, bool read_only)
  : _fd(-1), _mode(SqliteDb::FAIL)
{
  // check if file exists on path
  struct stat st;
  bool file_exists = (stat(path, &st) == 0);
  int stat_errno = errno;

  if (file_exists) {
    // check if file is valid SQLite DB
    int fd = open(path, O_RDONLY);
    if (st.st_size > 0 && !has_sqlite3_signature(fd)) {
      _mode = SqliteDb::FAIL;  // (2) pathにファイルがあるが，SQLiteのDBとしてinvalid (注: 空ファイルはvalid)
      log_msg("Invalid SQLite database format: %s\n", path);
      close(fd);
      return;
    }
    close(fd);
  }

  if (read_only && !file_exists) {
    _mode = SqliteDb::FAIL;  // (3) READ_ONLYモードでDBを開くようにリクエストされたが，ファイルが存在しない
    char err[256];
    log_msg("Cannot open %s in read-only mode. %s\n", path, strerror_r(stat_errno, err, 256));
    return;
  }
  else if (read_only && file_exists) {
    _mode = SqliteDb::READ_ONLY;  // (1) READ_ONLYモードでDBを開くようにリクエストされ，validなDBを開いた
    _fd = open(path, O_RDONLY);
  }
  else if (!read_only && file_exists) {
    _mode = SqliteDb::READ_WRITE;  // (1) pathにvalidなSQLite DBがあって，それを通常モードで開いた
    _fd = open(path, O_RDWR);
  }
  else if (!read_only && !file_exists) {
    _fd = open(path, O_RDWR | O_CREAT);    // try to create new db
    int open_errno = errno;
    if (_fd != -1) {
      _mode = SqliteDb::READ_WRITE; // (2) pathのディレクトリ上で新規にSQLite DBを作成した
    } else /* if (failed to create new db) */ {
      _mode = SqliteDb::FAIL; // (1) pathのディレクトリ上で新規にファイル作成できない
      char err[256];
      log_msg("Cannot create %s. %s\n", path, strerror_r(open_errno, err, 256));
      return;
    }
  }
  else assert(false);
}
SqliteDb::~SqliteDb()
{
  if (_mode == SqliteDb::FAIL) return;

  if (close(_fd) != 0) {
    char err[256];
    log_msg("Db file (fd=%d) cannot be closed. %s\n", _fd, strerror_r(errno, err, 256));
  }
}

SqliteDb::open_mode SqliteDb::mode() const
{
  return _mode;
}


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

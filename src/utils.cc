#include <cerrno>

#include "utils.h"


/***********************************************************************
** SqliteDb class
***********************************************************************/
 bool SqliteDb::has_sqlite3_signature(int fd)
{
  char s[SQLITE3_SIGNATURE_SZ];
  if (MYSQLITE_OK != mysqlite_read(s, 0, SQLITE3_SIGNATURE_SZ, fd)) return false;
  return strcmp(s, SQLITE3_SIGNATURE) == 0;
}

SqliteDb::SqliteDb(const char *path, bool read_only)
  : _fd(-1), _mode(SqliteDb::FAIL)
{
  // check if file exists on path
  bool file_exists = (stat(path, &_file_stat) == 0);
  int stat_errno = errno;

  if (file_exists) {
    // check if file is valid SQLite DB
    int fd = open(path, O_RDONLY);
    if (_file_stat.st_size > 0 && !has_sqlite3_signature(fd)) {
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

size_t SqliteDb::file_size() const
{
  return _file_stat.st_size;
}

int SqliteDb::fd() const
{
  return _fd;
}

SqliteDb::open_mode SqliteDb::mode() const
{
  return _mode;
}

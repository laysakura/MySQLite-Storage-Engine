/* Copyright (c) Sho Nakatani 2013. All rights reserved.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */


#ifndef _UTILS_H_
#define _UTILS_H_


using namespace std;
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#include "mysqlite_types.h"


/*
  Assertion
 */
// Suppress warning for -DNDEBUG build
#define my_assert(expr) do {assert(expr); (void)(expr);} while (0)


/*
** Logger
*/
#define log_msg(fmt, ...)                                                   \
  do {                                                                  \
    time_t _t = time(NULL);                                             \
    tm _tm;                                                             \
    localtime_r(&_t, &_tm);                                             \
    fprintf(stderr,                                                     \
            "%02d%02d%02d %02d:%02d:%02d ha_mysqlite: " __FILE__ ":%d: " fmt, \
            _tm.tm_year % 100, _tm.tm_mon + 1, _tm.tm_mday,             \
            _tm.tm_hour, _tm.tm_min, _tm.tm_sec,                        \
            __LINE__, ## __VA_ARGS__);                                  \
  } while (0)

#define case_log_errstat(errstat, msg)   \
  case errstat:                          \
    log_msg(msg);                        \
    break;                               \

#define log_errstat(errstat)                                            \
  do {                                                                  \
    switch (errstat) {                                                  \
    case MYSQLITE_OK:                                                   \
      break;                                                            \
                                                                         \
    /* Messages */                                                       \
    case_log_errstat(MYSQLITE_CORRUPT_DB, "Database is corrupt\n");      \
    case_log_errstat(MYSQLITE_PERMISSION_DENIED, "Permission denied\n"); \
    case_log_errstat(MYSQLITE_NO_SUCH_TABLE, "No such table\n");         \
    case_log_errstat(MYSQLITE_IO_ERR, "File IO error\n");                \
    case_log_errstat(MYSQLITE_DB_FILE_NOT_FOUND, "DB file is not found\n"); \
    case_log_errstat(MYSQLITE_OUT_OF_MEMORY, "Out of memory\n");        \
    case_log_errstat(MYSQLITE_CONNECTION_ALREADY_OPEN, "Connection is already open\n"); \
    case_log_errstat(MYSQLITE_FLOCK_NEEDED, "File lock is necessary\n");        \
    case_log_errstat(MYSQLITE_CANNOT_OPEN_DB_FILE, "Failed to open file as SQLite3 DB\n"); \
                                                                        \
    default:                                                            \
      log_msg("!!! errstat=%d has no corresponding message !!!\n", errstat); \
      abort();                                                          \
    }                                                                   \
  } while (0)


/*
** Read (v[0] | v[1] ...) as a variant.
** If MSB of v[i] is 0, then v[i+1] is ignored.
**
** @note
** See varint format (very simple):
** http://www.sqlite.org/fileformat2.html - 'A variable-length integer ...'
*/
static inline u64 varint2u64(u8 *v,
                             /* out */
                             u8 *len)
{
  u64 res = v[0] & 127;
  if ((v[0] & 128) == 0) {
    *len = 1;
    return res;
  }
  for (u8 i = 1; i < 8; ++i) {
    res = (res << 7) + (v[i] & 127);
    if ((v[i] & 128)  == 0) {
      *len = i + 1;
      return res;
    }
  }
  // 9th byte has 8 effective bits
  res = (res << 7) + v[8];
  *len = 9;
  return res;
}
static inline u64 varint2u64(u8 *v)
{
  u8 tmp;
  return varint2u64(v, &tmp);
}


/*
** Read 1-4 u8 values as big-endian value.
*/
template<typename T>
T u8s_to_val(const u8 * const p_sequence, u8 len_sequence) {
  my_assert(len_sequence > 0);
  T v = 0;
  for (u8 i = 0; i < len_sequence; ++i)
    v = (v << 8) + p_sequence[i];
  return v;
}


static inline errstat mysqlite_fread(void *ptr, long offset, size_t nbyte, FILE * const f) {
  if ((ssize_t)nbyte != pread(fileno(f), ptr, nbyte, offset)) {
    perror("pread() fails\n");
    return MYSQLITE_IO_ERR;
  }
  return MYSQLITE_OK;
}


/**
 * Class to deal with SQLite DB file written by RAII idiom.
 *
 * This *only* provides:
 * - Error check
 * - File descriptor
 * - Open mode
 */
class SqliteDb {
public:
  typedef enum {
    FAIL,        // (1) pathのディレクトリ上で新規にファイル作成できない
                 // (2) pathにファイルがあるが，SQLiteのDBとしてinvalid (注: 空ファイルはvalid)
                 // (3) READ_ONLYモードでDBを開くようにリクエストされたが，ファイルが存在しない
    READ_WRITE,  // (1) pathにvalidなSQLite DBがあって，それを通常モードで開いた
                 // (2) pathのディレクトリ上で新規にSQLite DBを作成した
    READ_ONLY,   // (1) READ_ONLYモードでDBを開くようにリクエストされ，validなDBを開いた
  } open_mode;

private:
  struct stat _file_stat;
  int _fd;
  open_mode _mode;

  public:
  static bool has_sqlite3_signature(int fd);

  public:
  SqliteDb(const char *path, bool read_only = false);
  ~SqliteDb();

  public:
  int fd() const;
  size_t file_size() const;
  open_mode mode() const;

private:
  // Prohibit any way to create instance
  SqliteDb();
  SqliteDb(const SqliteDb&);
  SqliteDb& operator=(const SqliteDb&);
};


#endif /* _UTILS_H_ */

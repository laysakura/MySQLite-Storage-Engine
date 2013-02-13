#ifndef _UTILS_H_
#define _UTILS_H_


using namespace std;
#include <vector>


/*
** Types
*/
typedef signed char        s8;
typedef unsigned char      u8;
typedef signed short       s16;
typedef unsigned short     u16;
typedef signed int         s32;
typedef unsigned int       u32;
typedef signed long long   s64;
typedef unsigned long long u64;
typedef vector<u8> varint;


/*
** Logger
*/
#define log(fmt, ...)                                                   \
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


/*
** Read (v[0] | v[1] ...) as a variant.
** If MSB of v[i] is 0, then v[i+1] is ignored.
**
** @note
** See varint format (very simple):
** http://www.sqlite.org/fileformat2.html - 'A variable-length integer ...'
*/
static inline u64 varint2u64(varint &v)
{
  u64 res = v[0] & 127;
  if ((v[0] & 128) == 0) return res;
  for (int i = 1; i < 8; ++i) {
    res = (res << 7) + (v[i] & 127);
    if ((v[i] & 128)  == 0) return res;
  }
  // 9th byte has 8 effective bits
  res = (res << 7) + v[8];
  return res;
}


#endif /* _UTILS_H_ */

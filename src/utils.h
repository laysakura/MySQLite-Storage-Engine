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
** Class to prohibit copy constructor
*/
class Uncopyable {
protected:
  Uncopyable() {}
  ~Uncopyable() {}
private:
    Uncopyable(const Uncopyable&);
    Uncopyable& operator=(const Uncopyable&);
};


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
  for (int i = 1; i < 8; ++i) {
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
T u8s_to_val(const u8 * const p_sequence, int len_sequence) {
  T v = 0;
  for (int i = 0; i < len_sequence; ++i)
    v = (v << 8) + p_sequence[i];
  return v;
}


static inline bool mysqlite_fread(void *ptr, long offset, size_t nbyte, FILE * const f) {
  long prev_offset = ftell(f);
  if (fseek(f, offset, SEEK_SET) == -1) {
    perror("fseek() failed\n");
    return false;
  }
  if (nbyte != fread(ptr, 1, nbyte, f)) {
    perror("fread() fails\n");
    return false;
  }
  if (fseek(f, prev_offset, SEEK_SET) == -1) {
    perror("fseek() failed\n");
    return false;
  }
  return true;
}


#endif /* _UTILS_H_ */

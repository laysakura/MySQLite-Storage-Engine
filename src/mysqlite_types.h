#ifndef _MYSQLITE_TYPES_H_
#define _MYSQLITE_TYPES_H_


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
** Error status.
*/
enum errstat {
  MYSQLITE_OK = 0,
  MYSQLITE_CORRUPT_DB = 1,
  MYSQLITE_PERMISSION_DENIED = 2,
  MYSQLITE_NO_SUCH_TABLE = 3,
  MYSQLITE_IO_ERR = 4,
  MYSQLITE_DB_FILE_NOT_FOUND = 5,
};


#endif /* _MYSQLITE_TYPES_H_ */

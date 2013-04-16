#ifndef _MYSQLITE_TYPES_H_
#define _MYSQLITE_TYPES_H_


#include <stdlib.h>

/*
  Basic utility types
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
  MySQLite API types.

  @see http://www.sqlite.org/c3ref/c_blob.html
*/
typedef enum mysqlite_type {
  MYSQLITE_INTEGER = 1,
  MYSQLITE_FLOAT   = 2,
  MYSQLITE_TEXT    = 3,
  MYSQLITE_BLOB    = 4,
  MYSQLITE_NULL    = 5,
} mysqlite_type;

/*
  Error status.
*/
enum errstat {
  MYSQLITE_OK = 0,
  MYSQLITE_CORRUPT_DB = 1,
  MYSQLITE_PERMISSION_DENIED = 2,
  MYSQLITE_NO_SUCH_TABLE = 3,
  MYSQLITE_IO_ERR = 4,
  MYSQLITE_DB_FILE_NOT_FOUND = 5,
  MYSQLITE_OUT_OF_MEMORY = 6,
};

/*
  SQLite internal types
*/
typedef u32 Pgno;
typedef u16 Pgsz;
typedef u64 Rowid;

typedef enum btree_page_type {
  INDEX_INTERIOR     = 2,
  TABLE_INTERIOR     = 5,
  INDEX_LEAF         = 10,
  TABLE_LEAF         = 13,
} btree_page_type;

typedef enum sqlite_type {
  ST_NULL =  0,
  ST_INT8 =  1,
  ST_INT16 = 2,
  ST_INT24 = 3,
  ST_INT32 = 4,
  ST_INT48 = 5,
  ST_INT64 = 6,
  ST_FLOAT = 7,
  ST_C0 =    8,
  ST_C1 =    9,
  ST_BLOB,   /* >= 12, even */
  ST_TEXT,   /* >= 13, odd  */
} sqlite_type;

static inline mysqlite_type sqlite_type_to_mysqlite_type(sqlite_type st)
{
  switch (st) {
  case ST_NULL:
    return MYSQLITE_NULL;
  case ST_C0:
  case ST_C1:
  case ST_INT8:
  case ST_INT16:
  case ST_INT24:
  case ST_INT32:
    return MYSQLITE_INTEGER;
  case ST_INT48:
  case ST_INT64:
    abort();  // TODO: support 64bit column int value.
  case ST_FLOAT:
    return MYSQLITE_FLOAT;
  case ST_BLOB:
    return MYSQLITE_BLOB;
  case ST_TEXT:
    return MYSQLITE_TEXT;
  }
  // Just for avoiding warning
  return MYSQLITE_NULL;
}


#endif /* _MYSQLITE_TYPES_H_ */

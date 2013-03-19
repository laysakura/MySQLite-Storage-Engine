#ifndef _MYSQLITE_TYPES_H_
#define _MYSQLITE_TYPES_H_


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
};


#endif /* _MYSQLITE_TYPES_H_ */

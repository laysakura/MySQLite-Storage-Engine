using namespace std;
#include <list>
#include <string>

/*
** Utils
*/
#include "sqlite_format.h"

/*
**
*/
static inline bool has_sqlite3_header(FILE *f)
{
  char s[SQLITE3_HEADER_SIZE];
  long prev_offset = ftell(f);
  rewind(f);
  if (fread(s, 1, SQLITE3_HEADER_SIZE, f) != SQLITE3_HEADER_SIZE) {
    log("fread() fails\n");
    return false;
  }
  if (fseek(f, prev_offset, SEEK_SET) == -1) {
    perror("has_sqlite3_header");
    return false;
  }
  return strcmp(s, SQLITE3_HEADER) == 0;
}

/*
** pathをopenし，もし存在すればそれがSQLite DBとしてvalidなものかをチェックする
**
** @return
** NULL: Error. `message' is set.
*/
static inline FILE *open_sqlite_db(char *path,
                                   /* out */
                                   bool *is_existing_db, char *message)
{
  struct stat st;
  if (stat(path, &st) == 0) {
    *is_existing_db = true;
    FILE *f = fopen(path, "r");
    if (!f) {
      sprintf(message, "Permission denied: Cannot open %s in read mode.", path);
      return NULL;
    }
    if (!has_sqlite3_header(f)) {
      sprintf(message, "Format error: %s does not seem SQLite3 database.", path);
      return NULL;
    }
    return f;
  } else {
    *is_existing_db = false;
    FILE *f = fopen(path, "w+");
    if (!f) {
      sprintf(message, "Permission denied: Cannot create %s.", path);
      return NULL;
    }
    return f;
  }
}

bool copy_sqlite_ddls(FILE *f_db,
                      /* out */
                      list<string> &ddls)
{
  // TODO: Somehow vector.push_back fails in runtime
  // saying "undefined symbol: ..."
  ddls.push_back("create table hhh02_table1_ddl_short_t1 (c1 INT)");
  return true;
}

#include <mysql.h>
bool dup_table_schema(FILE *f_db)
{
  MYSQL *conn;
  if (!(conn = mysql_init(NULL))) {
    log("Error %u: %s\n", mysql_errno(conn), mysql_error(conn));
    goto err_ret;
  }
  // TODO: BUG: Connection should not be newly created.
  // By reusing connection, these 'root', 'mysql.sock' things will diminish.
  if (conn != mysql_real_connect(conn, "localhost", "root", "", "test", 0, "/tmp/mysql.sock", 0)) {
    log("Error %u: %s\n", mysql_errno(conn), mysql_error(conn));
    goto err_ret;
  }

  { // Duplicate SQLite DDLs to MySQL
    list<string> ddls;
    if (!copy_sqlite_ddls(f_db, ddls))
      goto err_ret;

    for (list<string>::iterator it = ddls.begin(); it != ddls.end(); ++it) {
      if (mysql_query(conn, it->c_str())) {
        log("Error %u: %s\n", mysql_errno(conn), mysql_error(conn));
        goto err_ret;
      }
    }
  }
  mysql_close(conn);
  return true;

err_ret:
  mysql_close(conn);
  return false;
}

/*
** この関数(sqlite_db_init)は，SQLite DBを開き，validityのチェックを行うところまで．
**
** やりたいこと
** 1. 要求されたsqlite dbファイルが存在しない時: 作成する
** 2. 存在する時: テーブル定義を読み取って.frmをupdateする
**
** @example
** -- ex1:
** select sqlite_db('/path/to/foo.sqlite');  -- foo.sqliteは存在しない
** create table T0 (...) engine=mysqlite;    -- foo.sqliteができ，その中にT0というテーブルができる
**
** -- ex2:
** select sqlite_db('/path/to/bar.sqlite');  -- bar.sqliteは存在し，既にS0,S1というテーブルもある．
**     -- この時点で現在useしてるMySQL側のDBに，S0,S1というテーブルの定義{S0,S1}.frmもできる．
**
** @param (char *)initid->ptr: Points to a byte where
**   0 indicates new DB is created and
**   1 indicates existing DB is read.
** @param (void *)initid->extension: File descriptor to DB.
*/
my_bool sqlite_db_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  if (args->arg_count == 0 || args->arg_count >= 2) {
    strcpy(message, "sqlite_schema('/path/to/sqlite_db'): argument error");
    return 1;
  }
  args->arg_type[0] = STRING_RESULT;
  args->maybe_null[0] = 0;
  initid->maybe_null = 1;

  char *path = args->args[0];
  bool *is_existing_db = (bool *)malloc(1);
  initid->extension = open_sqlite_db(path, is_existing_db, message);
  if (!initid->extension) return 1;

  initid->ptr = (char *)is_existing_db;
  return 0;
}

void sqlite_db_deinit(UDF_INIT *initid __attribute__((unused)))
{
  FILE *f = (FILE *)initid->extension;
  DBUG_ASSERT(f);
  fclose(f);

  bool *is_existing_db = (bool *)initid->ptr;
  free(is_existing_db);
}

/*
** sqlite db(validityチェック済み)が存在すれば，そのtable schemaを見てCREATE TABLEを発行
**
** @return
** 0: Creating new DB in write mode.
** 1: Opening existing DB in read mode.
*/
long long sqlite_db(UDF_INIT *initid __attribute__((unused)), UDF_ARGS *args,
                    char *is_null, char *error)
{
  FILE *f_db = (FILE *)initid->extension;
  bool is_existing_db = *((bool *)initid->ptr);

  if (is_existing_db) {
    dup_table_schema(f_db);
  }

  *is_null = 0;
  return is_existing_db;
}

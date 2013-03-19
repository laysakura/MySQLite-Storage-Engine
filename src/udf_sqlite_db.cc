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


using namespace std;
#include <vector>
#include <string>

#include <sql_priv.h>
#include <sql_class.h>
#include <sql_plugin.h>

#include "udf_sqlite_db.h"
#include "sqlite_format.h"
#include "utils.h"
#include "mysqlite_api.h"
#include "ha_mysqlite.h"

bool copy_sqlite_table_formats(/* out */
                               vector<string> &table_names,
                               vector<string> &ddls)
{
  using namespace mysqlite;

  Mysqlite_share *share = Mysqlite_share::get_share();
  assert(share->conn.is_opened());

  RowCursor *rows = share->conn.table_fullscan("sqlite_master");
  assert(rows);

  while (rows->next()) {
    table_names.push_back(rows->get_text(SQLITE_MASTER_COLNO_NAME));
    ddls.push_back(rows->get_text(SQLITE_MASTER_COLNO_SQL));
  }

  rows->close();
  return true;


  // DbHeader db_header(f_db);
  // if (!db_header.read()) {
  //   log_msg("Cannot read DB header\n");
  //   return false;
  // }

  // TableLeafPage page1(f_db, &db_header, 1); // sqlite_master
  // if (!page1.read()) {
  //   log_msg("Cannot read page\n");
  //   return false;
  // }

  // {
  //   Pgsz n_cell = page1.get_n_cell();
  //   for (u64 cell = 0; cell < n_cell; ++cell) {
  //     u64 rowid;
  //     Pgno overflow_pgno;
  //     u64 overflown_payload_sz;
  //     vector<Pgsz> cols_offset, cols_len;
  //     vector<sqlite_type> cols_type;

  //     page1.get_ith_cell_cols(
  //       cell,
  //       &rowid,
  //       &overflow_pgno, &overflown_payload_sz,
  //       cols_offset,
  //       cols_len,
  //       cols_type);

  //     // Table name
  //     assert(cols_type[SQLITE_MASTER_COLNO_SQL] == ST_TEXT);
  //     table_names.push_back(
  //       string((char *)&page1.pg_data[cols_offset[SQLITE_MASTER_COLNO_NAME]],
  //              cols_len[SQLITE_MASTER_COLNO_NAME]));

  //     // Table DDL
  //     assert(cols_type[SQLITE_MASTER_COLNO_SQL] == ST_TEXT);
  //     ddls.push_back(
  //       string((char *)&page1.pg_data[cols_offset[SQLITE_MASTER_COLNO_SQL]],
  //              cols_len[SQLITE_MASTER_COLNO_SQL]));
  //   }
  // }
  // return true;
}

#include <mysql.h>
bool dup_table_schema(FILE *f_db)
{
  MYSQL *conn;
  if (!(conn = mysql_init(NULL))) {
    log_msg("Error %u: %s\n", mysql_errno(conn), mysql_error(conn));
    goto err_ret;
  }
  // TODO: BUG: Connection should not be newly created.
  // By reusing connection, these 'root', 'mysql.sock' things will diminish.
  if (conn != mysql_real_connect(conn, "localhost", "root", "", "test", 0, "/tmp/mysql.sock", 0)) {
    log_msg("Error %u: %s\n", mysql_errno(conn), mysql_error(conn));
    goto err_ret;
  }

  { // Duplicate SQLite DDLs to MySQL
    vector<string> table_names, ddls;
    if (!copy_sqlite_table_formats(table_names, ddls))
      goto err_ret;

    // Drop all tables defined in SQLite DB first (for updating .FRM)
    for (vector<string>::iterator it = table_names.begin();
         it != table_names.end(); ++it)
    {
      string sql = string("drop table if exists ") + *it;
      if (mysql_query(conn, sql.c_str())) {
        log_msg("Error %u: %s\n", mysql_errno(conn), mysql_error(conn));
        goto err_ret;
      }
    }

    // Create tables
    for (vector<string>::iterator it = ddls.begin(); it != ddls.end(); ++it) {
      if (mysql_query(conn, it->c_str())) {
        log_msg("Error %u: %s\n", mysql_errno(conn), mysql_error(conn));
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
  errstat res;
  initid->extension = _open_sqlite_db(path, is_existing_db, message, &res);
  if (!initid->extension) {
    log_errstat(res);
    return 1;
  }

  initid->ptr = (char *)is_existing_db;
  return 0;
}

void sqlite_db_deinit(UDF_INIT *initid __attribute__((unused)))
{
  FILE *f = (FILE *)initid->extension;
  DBUG_ASSERT(f);
  if (0 != fclose(f)) perror("fclose() failed\n");

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
long long sqlite_db(UDF_INIT *initid, UDF_ARGS *args,
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

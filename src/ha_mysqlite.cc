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

/**
  @file ha_mysqlite.cc

  Example storage engine bundled in MySQL source is used as a template.

  @brief
  The ha_mysqlite engine is a stubbed storage engine for example purposes only;
  it does nothing at this point. Its purpose is to provide a source
  code illustration of how to begin writing new storage engines; see also
  ha_mysqlite.h.

  @details
  ha_mysqlite will let you create/open/delete tables, but
  nothing further (for example, indexes are not supported nor can data
  be stored in the table). Use this example as a template for
  implementing the same functionality in your own storage engine. You
  can enable the mysqlite storage engine in your build by doing the
  following during your build process:<br> ./configure
  --with-mysqlite-storage-engine

  Once this is done, MySQL will let you create tables with:<br>
  CREATE TABLE <table name> (...) ENGINE=MYSQLITE;

  The mysqlite storage engine is set up to use table locks. It
  implements an example "SHARE" that is inserted into a hash by table
  name. You can use this to store information of state that any
  example handler object will be able to see when it is using that
  table.

  Please read the object definition in ha_mysqlite.h before reading the rest
  of this file.

  @note
  When you create an MYSQLITE table, the MySQL Server creates a table .frm
  (format) file in the database directory, using the table name as the file
  name as is customary with MySQL. No other files are created. To get an idea
  of what occurs, here is an example select that would do a scan of an entire
  table:

  @code
  ha_mysqlite::store_lock
  ha_mysqlite::external_lock
  ha_mysqlite::info
  ha_mysqlite::rnd_init
  ha_mysqlite::extra
  ENUM HA_EXTRA_CACHE        Cache record in HA_rrnd()
  ha_mysqlite::rnd_next
  ha_mysqlite::rnd_next
  ha_mysqlite::rnd_next
  ha_mysqlite::rnd_next
  ha_mysqlite::rnd_next
  ha_mysqlite::rnd_next
  ha_mysqlite::rnd_next
  ha_mysqlite::rnd_next
  ha_mysqlite::rnd_next
  ha_mysqlite::extra
  ENUM HA_EXTRA_NO_CACHE     End caching of records (def)
  ha_mysqlite::external_lock
  ha_mysqlite::extra
  ENUM HA_EXTRA_RESET        Reset database to after open
  @endcode

  Here you see that the mysqlite storage engine has 9 rows called before
  rnd_next signals that it has reached the end of its data. Also note that
  the table in question was already opened; had it not been open, a call to
  ha_mysqlite::open() would also have been necessary. Calls to
  ha_mysqlite::extra() are hints as to what will be occuring to the request.

  A Longer Example can be found called the "MySQLite Storage Engine" which can be 
  found on TangentOrg. It has both an engine and a full build environment
  for building a pluggable storage engine.

  Happy coding!<br>
    -Brian
*/

#undef SAFE_MUTEX  // TODO: Necessary to use correct TABLE_SHARE::LOCK_ha_data->m_mutex.
                   // TODO: But should not be undefed.
#include "ha_mysqlite.h"
#include "pcache.h"
#include "utils.h"
#include "mysqlite_api.h"
#include "mysqlite_config.h"

#include "sql_class.h"


/* Stuff for shares */
mysql_mutex_t mysqlite_mutex;
static HASH mysqlite_open_tables;
static handler *mysqlite_create_handler(handlerton *hton,
                                       TABLE_SHARE *table, 
                                       MEM_ROOT *mem_root);

handlerton *mysqlite_hton;

/* Interface to mysqld, to check system tables supported by SE */
static const char* mysqlite_system_database();
static bool mysqlite_is_supported_system_table(const char *db,
                                      const char *table_name,
                                      bool is_sql_layer_system_table);

static int mysqlite_assisted_discovery(handlerton *hton, THD* thd,
                                       TABLE_SHARE *table_s,
                                       HA_CREATE_INFO *info);

/**
  structure for CREATE TABLE options (table options)

  These can be specified in the CREATE TABLE:
  CREATE TABLE ( ... ) {...here...}
*/
struct ha_table_option_struct {
  const char *type;
  const char *filename;
  const char *optname;
  const char *tabname;
  const char *tablist;
  const char *dbname;
  const char *separator;
//const char *connect;
  const char *qchar;
  const char *module;
  const char *subtype;
  const char *catfunc;
  const char *oplist;
  const char *data_charset;
  ulonglong lrecl;
  ulonglong elements;
//ulonglong estimate;
  ulonglong multiple;
  ulonglong header;
  ulonglong quoted;
  ulonglong ending;
  ulonglong compressed;
  bool mapped;
  bool huge;
  bool split;
  bool readonly;
  bool sepindex;
  };

ha_create_table_option mysqlite_table_option_list[]=
{
  // These option are for stand alone Connect tables
  HA_TOPTION_STRING("TABLE_TYPE", type),
  HA_TOPTION_STRING("FILE_NAME", filename),
  HA_TOPTION_STRING("XFILE_NAME", optname),
//HA_TOPTION_STRING("CONNECT_STRING", connect),
  HA_TOPTION_STRING("TABNAME", tabname),
  HA_TOPTION_STRING("TABLE_LIST", tablist),
  HA_TOPTION_STRING("DBNAME", dbname),
  HA_TOPTION_STRING("SEP_CHAR", separator),
  HA_TOPTION_STRING("QCHAR", qchar),
  HA_TOPTION_STRING("MODULE", module),
  HA_TOPTION_STRING("SUBTYPE", subtype),
  HA_TOPTION_STRING("CATFUNC", catfunc),
  HA_TOPTION_STRING("OPTION_LIST", oplist),
  HA_TOPTION_STRING("DATA_CHARSET", data_charset),
  HA_TOPTION_NUMBER("LRECL", lrecl, 0, 0, INT_MAX32, 1),
  HA_TOPTION_NUMBER("BLOCK_SIZE", elements, 0, 0, INT_MAX32, 1),
//HA_TOPTION_NUMBER("ESTIMATE", estimate, 0, 0, INT_MAX32, 1),
  HA_TOPTION_NUMBER("MULTIPLE", multiple, 0, 0, 2, 1),
  HA_TOPTION_NUMBER("HEADER", header, 0, 0, 3, 1),
  HA_TOPTION_NUMBER("QUOTED", quoted, -1, 0, 3, 1),
  HA_TOPTION_NUMBER("ENDING", ending, -1, 0, INT_MAX32, 1),
  HA_TOPTION_NUMBER("COMPRESS", compressed, 0, 0, 2, 1),
//HA_TOPTION_BOOL("COMPRESS", compressed, 0),
  HA_TOPTION_BOOL("MAPPED", mapped, 0),
  HA_TOPTION_BOOL("HUGE", huge, 0),
  HA_TOPTION_BOOL("SPLIT", split, 0),
  HA_TOPTION_BOOL("READONLY", readonly, 0),
  HA_TOPTION_BOOL("SEPINDEX", sepindex, 0),
  HA_TOPTION_END
};

/**
  structure for CREATE TABLE options (field options)

  These can be specified in the CREATE TABLE per field:
  CREATE TABLE ( field ... {...here...}, ... )
*/
struct ha_field_option_struct
{
  ulonglong offset;
  ulonglong freq;      // Not used by this version
  ulonglong opt;       // Not used by this version
  ulonglong fldlen;
  const char *dateformat;
  const char *fieldformat;
  char *special;
};

ha_create_table_option mysqlite_field_option_list[]=
{
  HA_FOPTION_NUMBER("FLAG", offset, -1, 0, INT_MAX32, 1),
  HA_FOPTION_NUMBER("FREQUENCY", freq, 0, 0, INT_MAX32, 1), // not used
  HA_FOPTION_NUMBER("OPT_VALUE", opt, 0, 0, 2, 1),  // used for indexing
  HA_FOPTION_NUMBER("FIELD_LENGTH", fldlen, 0, 0, INT_MAX32, 1),
  HA_FOPTION_STRING("DATE_FORMAT", dateformat),
  HA_FOPTION_STRING("FIELD_FORMAT", fieldformat),
  HA_FOPTION_STRING("SPECIAL", special),
  HA_FOPTION_END
};


static uchar* mysqlite_get_key(Mysqlite_share *share, size_t *length,
                               my_bool not_used __attribute__((unused)))
{
  *length = strlen("global key");
  return (uchar *)"global key";
}

#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_key mysqlite_key_mutex;
static PSI_mutex_key mysqlite_key_mutex_Mysqlite_share_mutex;

static PSI_mutex_info all_mysqlite_mutexes[]=
{
  { &mysqlite_key_mutex, "mysqlite", PSI_FLAG_GLOBAL},
  { &mysqlite_key_mutex_Mysqlite_share_mutex, "Mysqlite_share::mutex", 0}
};

static void init_mysqlite_psi_keys()
{
  const char* category= "mysqlite";
  int count;

  count= array_elements(all_mysqlite_mutexes);
  PSI_server->register_mutex(category, all_mysqlite_mutexes, count);
}
#else
static void init_mysqlite_psi_keys() {}
#endif

Mysqlite_share::Mysqlite_share()
{
  thr_lock_init(&lock);

  abort();
  // mysql_mutex_init(mysqlite_key_mutex, &mysqlite_mutex, MY_MUTEX_INIT_FAST);
  // (void) my_hash_init(&mysqlite_open_tables, system_charset_info, 32, 0, 0,
  //                     (my_hash_get_key)mysqlite_get_key, 0, 0);
}


/**
  @brief
  If frm_error() is called then we will use this to determine
  the file extensions that exist for the storage engine. This is also
  used by the default rename_table and delete_table method in
  handler.cc.

  For engines that have two file name extentions (separate meta/index file
  and data file), the order of elements is relevant. First element of engine
  file name extentions array should be meta/index file extention. Second
  element - data file extention. This order is assumed by
  prepare_for_repair() when REPAIR TABLE ... USE_FRM is issued.

  @see
  rename_table method in handler.cc and
  delete_table method in handler.cc
*/

static const char *ha_mysqlite_exts[] = {
  ".sqlite", ".db", ".sqlite3",
};

const char **ha_mysqlite::bas_ext() const
{
  return ha_mysqlite_exts;
}


static int mysqlite_init_func(void *p)
{
  DBUG_ENTER("mysqlite_init_func");

  init_mysqlite_psi_keys();

  mysql_mutex_init(mysqlite_key_mutex, &mysqlite_mutex, MY_MUTEX_INIT_FAST);
  (void) my_hash_init(&mysqlite_open_tables, system_charset_info, 32, 0, 0,
                      (my_hash_get_key)mysqlite_get_key, 0, 0);

  mysqlite_hton= (handlerton *)p;
  mysqlite_hton->state=                     SHOW_OPTION_YES;
  mysqlite_hton->create=                    mysqlite_create_handler;
  mysqlite_hton->flags=   HTON_TEMPORARY_NOT_SUPPORTED | HTON_NO_PARTITION;
#ifndef MARIADB
  // TODO: Correspoinding vars for MariaDB?
  mysqlite_hton->system_database=   mysqlite_system_database;
  mysqlite_hton->is_supported_system_table= mysqlite_is_supported_system_table;
#else
  mysqlite_hton->table_options= mysqlite_table_option_list;
  mysqlite_hton->field_options= mysqlite_field_option_list;
  mysqlite_hton->tablefile_extensions= ha_mysqlite_exts;
  mysqlite_hton->discover_table_structure= mysqlite_assisted_discovery;
#endif //MARIADB

  // Page cache
  PageCache *pcache = PageCache::get_instance();
  pcache->alloc(MYSQLITE_PCACHE_SZ);

  DBUG_RETURN(0);
}

static int mysqlite_done_func(void *p)
{
  my_hash_free(&mysqlite_open_tables);
  mysql_mutex_destroy(&mysqlite_mutex);

  // Page cache
  PageCache *pcache = PageCache::get_instance();
  pcache->free();

  return 0;
}

/**
  @brief
  Example of simple lock controls. The "share" it creates is a
  structure we will pass to each mysqlite handler. Do you have to have
  one of these? Well, you have pieces that are used for locking, and
  they are needed to function.
*/

Mysqlite_share *Mysqlite_share::get_share()
{
  Mysqlite_share *share;

  DBUG_ENTER("Mysqlite_share::get_share()");

  mysql_mutex_lock(&mysqlite_mutex);

  /*
    If share is not present in the hash, create a new share and
    initialize its members.
  */
  if (!(share = (Mysqlite_share*)my_hash_search(&mysqlite_open_tables,
                                                (const uchar *)"global key",
                                                strlen("global key"))))  // TODO: Per table share
  {
    if (!my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
                         &share, sizeof(*share),
                         NullS))
    {
      mysql_mutex_unlock(&mysqlite_mutex);
      return NULL;
    }

    share->use_count= 0;

    if (my_hash_insert(&mysqlite_open_tables, (uchar*) share))
      goto error;
    thr_lock_init(&share->lock);
    mysql_mutex_init(mysqlite_key_mutex_Mysqlite_share_mutex,
                     &share->mutex, MY_MUTEX_INIT_FAST);
  }

  mysql_mutex_unlock(&mysqlite_mutex);
  DBUG_RETURN(share);

error:
  mysql_mutex_unlock(&mysqlite_mutex);
  my_free(share);
  DBUG_RETURN(NULL);
}

/*
  Free lock controls.
*/
int Mysqlite_share::free_share(Mysqlite_share *share)
{
  DBUG_ENTER("Mysqlite_share::free_share");
  mysql_mutex_lock(&mysqlite_mutex);
  int result_code= 0;
  if (!--share->use_count) {
    /* nakatani: あるテーブルへのリファレンスカウントが0になったら，本格的にshareにまつわるデータをfreeしていく */
    share->conn.close();
    my_hash_delete(&mysqlite_open_tables, (uchar*) share);
    thr_lock_delete(&share->lock);
    mysql_mutex_destroy(&share->mutex);
    my_free(share);
  }
  mysql_mutex_unlock(&mysqlite_mutex);

  DBUG_RETURN(result_code);
}

static handler* mysqlite_create_handler(handlerton *hton,
                                       TABLE_SHARE *table, 
                                       MEM_ROOT *mem_root)
{
  return new (mem_root) ha_mysqlite(hton, table);
}

ha_mysqlite::ha_mysqlite(handlerton *hton, TABLE_SHARE *table_arg)
  :handler(hton, table_arg)
{
}


/*
  Following handler function provides access to
  system database specific to SE. This interface
  is optional, so every SE need not implement it.
*/
const char* ha_mysqlite_system_database= NULL;
const char* mysqlite_system_database()
{
  return ha_mysqlite_system_database;
}

/*
  List of all system tables specific to the SE.
  Array element would look like below,
     { "<database_name>", "<system table name>" },
  The last element MUST be,
     { (const char*)NULL, (const char*)NULL }

  This array is optional, so every SE need not implement it.
*/
// TODO: MariaDB alternative
#ifndef MARIADB
static st_system_tablename ha_mysqlite_system_tables[]= {
  {(const char*)NULL, (const char*)NULL}
};
#endif

/**
  @brief Check if the given db.tablename is a system table for this SE.

  @param db                         Database name to check.
  @param table_name                 table name to check.
  @param is_sql_layer_system_table  if the supplied db.table_name is a SQL
                                    layer system table.

  @return
    @retval TRUE   Given db.table_name is supported system table.
    @retval FALSE  Given db.table_name is not a supported system table.
 */
// TODO: MariaDB alternative
#ifndef MARIADB
static bool mysqlite_is_supported_system_table(const char *db,
                                              const char *table_name,
                                              bool is_sql_layer_system_table)
{
  st_system_tablename *systab;

  // Does this SE support "ALL" SQL layer system tables ?
  if (is_sql_layer_system_table)
    return false;

  // Check if this is SE layer system tables
  systab= ha_mysqlite_system_tables;
  while (systab && systab->db)
  {
    if (systab->db == db &&
        strcmp(systab->tablename, table_name) == 0)
      return true;
    systab++;
  }

  return false;
}
#endif

/**
  @brief
  Used for opening tables. The name will be the name of the file.

  @details
  A table is opened when it needs to be opened; e.g. when a request comes in
  for a SELECT on the table (tables are not open and closed for each request,
  they are cached).

  Called from handler.cc by handler::ha_open(). The server opens all tables by
  calling ha_open() which then calls the handler specific open().

  @see
  handler::ha_open() in handler.cc
*/

int ha_mysqlite::open(const char *name, int mode, uint test_if_locked)
{
  DBUG_ENTER("ha_mysqlite::open");

  if (!(share = Mysqlite_share::get_share()))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);

  thr_lock_data_init(&share->lock,&lock,NULL);

  DBUG_RETURN(0);
}


/**
  @brief
  Closes a table.

  @details
  Called from sql_base.cc, sql_select.cc, and table.cc. In sql_select.cc it is
  only used to close up temporary tables or during the process where a
  temporary table is converted over to being a myisam table.

  For sql_base.cc look at close_data_tables().

  @see
  sql_base.cc, sql_select.cc and table.cc
*/

int ha_mysqlite::close(void)
{
  DBUG_ENTER("ha_mysqlite::close");
  DBUG_RETURN(Mysqlite_share::free_share(share));
}


/**
  @brief
  write_row() inserts a row. No extra() hint is given currently if a bulk load
  is happening. buf() is a byte array of data. You can use the field
  information to extract the data from the native byte array type.

  @details
  Example of this would be:
  @code
  for (Field **field=table->field ; *field ; field++)
  {
    ...
  }
  @endcode

  See ha_tina.cc for an example of extracting all of the data as strings.
  ha_berekly.cc has an example of how to store it intact by "packing" it
  for ha_berkeley's own native storage type.

  See the note for update_row() on auto_increments. This case also applies to
  write_row().

  Called from item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
  sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc, and sql_update.cc.

  @see
  item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
  sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc and sql_update.cc
*/

int ha_mysqlite::write_row(uchar *buf)
{
  DBUG_ENTER("ha_mysqlite::write_row");
  /*
    Example of a successful write_row. We don't store the data
    anywhere; they are thrown away. A real implementation will
    probably need to do something with 'buf'. We report a success
    here, to pretend that the insert was successful.
  */
  DBUG_RETURN(0);
}


/**
  @brief
  Yes, update_row() does what you expect, it updates a row. old_data will have
  the previous row record in it, while new_data will have the newest data in it.
  Keep in mind that the server can do updates based on ordering if an ORDER BY
  clause was used. Consecutive ordering is not guaranteed.

  @details
  Currently new_data will not have an updated auto_increament record. You can
  do this for example by doing:

  @code

  if (table->next_number_field && record == table->record[0])
    update_auto_increment();

  @endcode

  Called from sql_select.cc, sql_acl.cc, sql_update.cc, and sql_insert.cc.

  @see
  sql_select.cc, sql_acl.cc, sql_update.cc and sql_insert.cc
*/
int ha_mysqlite::update_row(const uchar *old_data, uchar *new_data)
{

  DBUG_ENTER("ha_mysqlite::update_row");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  This will delete a row. buf will contain a copy of the row to be deleted.
  The server will call this right after the current row has been called (from
  either a previous rnd_nexT() or index call).

  @details
  If you keep a pointer to the last row or can access a primary key it will
  make doing the deletion quite a bit easier. Keep in mind that the server does
  not guarantee consecutive deletions. ORDER BY clauses can be used.

  Called in sql_acl.cc and sql_udf.cc to manage internal table
  information.  Called in sql_delete.cc, sql_insert.cc, and
  sql_select.cc. In sql_select it is used for removing duplicates
  while in insert it is used for REPLACE calls.

  @see
  sql_acl.cc, sql_udf.cc, sql_delete.cc, sql_insert.cc and sql_select.cc
*/

int ha_mysqlite::delete_row(const uchar *buf)
{
  DBUG_ENTER("ha_mysqlite::delete_row");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  Positions an index cursor to the index specified in the handle. Fetches the
  row if available. If the key value is null, begin at the first key of the
  index.
*/

int ha_mysqlite::index_read_map(uchar *buf, const uchar *key,
                               key_part_map keypart_map __attribute__((unused)),
                               enum ha_rkey_function find_flag
                               __attribute__((unused)))
{
  int rc;
  DBUG_ENTER("ha_mysqlite::index_read");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  Used to read forward through the index.
*/

int ha_mysqlite::index_next(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_mysqlite::index_next");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  Used to read backwards through the index.
*/

int ha_mysqlite::index_prev(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_mysqlite::index_prev");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  index_first() asks for the first key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_mysqlite::index_first(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_mysqlite::index_first");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  index_last() asks for the last key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_mysqlite::index_last(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_mysqlite::index_last");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  rnd_init() is called when the system wants the storage engine to do a table
  scan. See the example in the introduction at the top of this file to see when
  rnd_init() is called.

  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc,
  and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and sql_update.cc
*/
int ha_mysqlite::rnd_init(bool scan)
{
  DBUG_ENTER("ha_mysqlite::rnd_init");

  // share->conn should be already opened by UDF sqlite_db().
  if (!share->conn.is_opened()) {
    log_msg("No SQLite DB file is opend.\n");
    abort();    // TODO: More decent way to report SQLite db is not opened.
  }

  rows = share->conn.table_fullscan(/*table_share->table_name.str*/"T0");
  my_assert(rows);

  DBUG_RETURN(0);
}

int ha_mysqlite::rnd_end()
{
  DBUG_ENTER("ha_mysqlite::rnd_end");

  rows->close();

  DBUG_RETURN(0);
}


/**
  @brief
  This is called for each row of the table scan. When you run out of records
  you should return HA_ERR_END_OF_FILE. Fill buff up with the row information.
  The Field structure for the table is the key to getting data into buf
  in a manner that will allow the server to understand it.

  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc,
  and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and sql_update.cc
*/
int ha_mysqlite::rnd_next(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_mysqlite::rnd_next");
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str,
                       TRUE);

  ha_statistic_increment(&SSV::ha_read_rnd_next_count);

  if ((rc = find_current_row(buf))) {
    /* nakatani: find_current_rowの中で実際のSQLite DBのパースを行い，
    ** nakatani: bufをMySQLのレコードフォーマットに合わせて埋めている．
    */
    /*
    ** rc!=0のときは，何かのエラーか，もうレコードを出しきった(HA_ERR_END_OF_FILE)時．
    */
    goto end;
  } else {
    stats.records++;
  }

end:
  MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


int ha_mysqlite::find_current_row(uchar *buf)
{
  if (!rows->next()) return HA_ERR_END_OF_FILE;

  /* Avoid asserts in ::store() for columns that are not going to be updated */
  my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->write_set);

  memset(buf, 0, table->s->null_bytes);  // TODO: Support NULL column

  for (Field **field=table->field ; *field ; field++) {
    int colno = (*field)->field_index;
    if (bitmap_is_set(table->read_set, colno)) {
      switch (rows->get_type(colno)) {
      case MYSQLITE_INTEGER:
        (*field)->store(rows->get_int(colno));
        break;
      case MYSQLITE_TEXT:
        {
          const char *s = rows->get_text(colno);
          (*field)->store(s, strlen(s), &my_charset_utf8_unicode_ci);  // TODO: Japanese support
        }
        break;
      default:
        abort();
      }
    }
  }

  dbug_tmp_restore_column_map(table->write_set, org_bitmap);
  return 0;
}


/**
  @brief
  position() is called after each call to rnd_next() if the data needs
  to be ordered. You can do something like the following to store
  the position:
  @code
  my_store_ptr(ref, ref_length, current_position);
  @endcode

  @details
  The server uses ref to store data. ref_length in the above case is
  the size needed to store current_position. ref is just a byte array
  that the server will maintain. If you are using offsets to mark rows, then
  current_position should be the offset. If it is a primary key like in
  BDB, then it needs to be a primary key.

  Called from filesort.cc, sql_select.cc, sql_delete.cc, and sql_update.cc.

  @see
  filesort.cc, sql_select.cc, sql_delete.cc and sql_update.cc
*/
void ha_mysqlite::position(const uchar *record)
{
  DBUG_ENTER("ha_mysqlite::position");
  DBUG_VOID_RETURN;
}


/**
  @brief
  This is like rnd_next, but you are given a position to use
  to determine the row. The position will be of the type that you stored in
  ref. You can use ha_get_ptr(pos,ref_length) to retrieve whatever key
  or position you saved when position() was called.

  @details
  Called from filesort.cc, records.cc, sql_insert.cc, sql_select.cc, and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_insert.cc, sql_select.cc and sql_update.cc
*/
int ha_mysqlite::rnd_pos(uchar *buf, uchar *pos)
{
  log_msg("enter rnd_pos\n");

  int rc;
  DBUG_ENTER("ha_mysqlite::rnd_pos");
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str,
                       TRUE);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  ::info() is used to return information to the optimizer. See my_base.h for
  the complete description.

  @details
  Currently this table handler doesn't implement most of the fields really needed.
  SHOW also makes use of this data.

  You will probably want to have the following in your code:
  @code
  if (records < 2)
    records = 2;
  @endcode
  The reason is that the server will optimize for cases of only a single
  record. If, in a table scan, you don't know the number of records, it
  will probably be better to set records to two so you can return as many
  records as you need. Along with records, a few more variables you may wish
  to set are:
    records
    deleted
    data_file_length
    index_file_length
    delete_length
    check_time
  Take a look at the public variables in handler.h for more information.

  Called in filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc,
  sql_delete.cc, sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_show.cc,
  sql_table.cc, sql_union.cc, and sql_update.cc.

  @see
  filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc, sql_delete.cc,
  sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_table.cc,
  sql_union.cc and sql_update.cc
*/
int ha_mysqlite::info(uint flag)
{
  DBUG_ENTER("ha_mysqlite::info");
  DBUG_RETURN(0);
}


/**
  @brief
  extra() is called whenever the server wishes to send a hint to
  the storage engine. The myisam engine implements the most hints.
  ha_innodb.cc has the most exhaustive list of these hints.

    @see
  ha_innodb.cc
*/
int ha_mysqlite::extra(enum ha_extra_function operation)
{
  DBUG_ENTER("ha_mysqlite::extra");
  DBUG_RETURN(0);
}


/**
  @brief
  Used to delete all rows in a table, including cases of truncate and cases where
  the optimizer realizes that all rows will be removed as a result of an SQL statement.

  @details
  Called from item_sum.cc by Item_func_group_concat::clear(),
  Item_sum_count_distinct::clear(), and Item_func_group_concat::clear().
  Called from sql_delete.cc by mysql_delete().
  Called from sql_select.cc by JOIN::reinit().
  Called from sql_union.cc by st_select_lex_unit::exec().

  @see
  Item_func_group_concat::clear(), Item_sum_count_distinct::clear() and
  Item_func_group_concat::clear() in item_sum.cc;
  mysql_delete() in sql_delete.cc;
  JOIN::reinit() in sql_select.cc and
  st_select_lex_unit::exec() in sql_union.cc.
*/
int ha_mysqlite::delete_all_rows()
{
  DBUG_ENTER("ha_mysqlite::delete_all_rows");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  Used for handler specific truncate table.  The table is locked in
  exclusive mode and handler is responsible for reseting the auto-
  increment counter.

  @details
  Called from Truncate_statement::handler_truncate.
  Not used if the handlerton supports HTON_CAN_RECREATE, unless this
  engine can be used as a partition. In this case, it is invoked when
  a particular partition is to be truncated.

  @see
  Truncate_statement in sql_truncate.cc
  Remarks in handler::truncate.
*/
int ha_mysqlite::truncate()
{
  DBUG_ENTER("ha_mysqlite::truncate");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  This create a lock on the table. If you are implementing a storage engine
  that can handle transacations look at ha_berkely.cc to see how you will
  want to go about doing this. Otherwise you should consider calling flock()
  here. Hint: Read the section "locking functions for mysql" in lock.cc to understand
  this.

  @details
  Called from lock.cc by lock_external() and unlock_external(). Also called
  from sql_table.cc by copy_data_between_tables().

  @see
  lock.cc by lock_external() and unlock_external() in lock.cc;
  the section "locking functions for mysql" in lock.cc;
  copy_data_between_tables() in sql_table.cc.
*/
int ha_mysqlite::external_lock(THD *thd, int lock_type)
{
  DBUG_ENTER("ha_mysqlite::external_lock");
  DBUG_RETURN(0);
}


/**
  @brief
  The idea with handler::store_lock() is: The statement decides which locks
  should be needed for the table. For updates/deletes/inserts we get WRITE
  locks, for SELECT... we get read locks.

  @details
  Before adding the lock into the table lock handler (see thr_lock.c),
  mysqld calls store lock with the requested locks. Store lock can now
  modify a write lock to a read lock (or some other lock), ignore the
  lock (if we don't want to use MySQL table locks at all), or add locks
  for many tables (like we do when we are using a MERGE handler).

  Berkeley DB, for example, changes all WRITE locks to TL_WRITE_ALLOW_WRITE
  (which signals that we are doing WRITES, but are still allowing other
  readers and writers).

  When releasing locks, store_lock() is also called. In this case one
  usually doesn't have to do anything.

  In some exceptional cases MySQL may send a request for a TL_IGNORE;
  This means that we are requesting the same lock as last time and this
  should also be ignored. (This may happen when someone does a flush
  table when we have opened a part of the tables, in which case mysqld
  closes and reopens the tables and tries to get the same locks at last
  time). In the future we will probably try to remove this.

  Called from lock.cc by get_lock_data().

  @note
  In this method one should NEVER rely on table->in_use, it may, in fact,
  refer to a different thread! (this happens if get_lock_data() is called
  from mysql_lock_abort_for_thread() function)

  @see
  get_lock_data() in lock.cc
*/
THR_LOCK_DATA **ha_mysqlite::store_lock(THD *thd,
                                       THR_LOCK_DATA **to,
                                       enum thr_lock_type lock_type)
{
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
    lock.type=lock_type;
  *to++= &lock;
  return to;
}


/**
  @brief
  Used to delete a table. By the time delete_table() has been called all
  opened references to this table will have been closed (and your globally
  shared references released). The variable name will just be the name of
  the table. You will need to remove any files you have created at this point.

  @details
  If you do not implement this, the default delete_table() is called from
  handler.cc and it will delete all files with the file extensions returned
  by bas_ext().

  Called from handler.cc by delete_table and ha_create_table(). Only used
  during create if the table_flag HA_DROP_BEFORE_CREATE was specified for
  the storage engine.

  @see
  delete_table and ha_create_table() in handler.cc
*/
int ha_mysqlite::delete_table(const char *name)
{
  DBUG_ENTER("ha_mysqlite::delete_table");
  /* This is not implemented but we want someone to be able that it works. */
  DBUG_RETURN(0);
}


/**
  @brief
  Renames a table from one name to another via an alter table call.

  @details
  If you do not implement this, the default rename_table() is called from
  handler.cc and it will delete all files with the file extensions returned
  by bas_ext().

  Called from sql_table.cc by mysql_rename_table().

  @see
  mysql_rename_table() in sql_table.cc
*/
int ha_mysqlite::rename_table(const char * from, const char * to)
{
  DBUG_ENTER("ha_mysqlite::rename_table ");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  Given a starting key and an ending key, estimate the number of rows that
  will exist between the two keys.

  @details
  end_key may be empty, in which case determine if start_key matches any rows.

  Called from opt_range.cc by check_quick_keys().

  @see
  check_quick_keys() in opt_range.cc
*/
ha_rows ha_mysqlite::records_in_range(uint inx, key_range *min_key,
                                     key_range *max_key)
{
  DBUG_ENTER("ha_mysqlite::records_in_range");
  DBUG_RETURN(10);                         // low number to force index usage
}


/**
  @brief
  create() is called to create a database. The variable name will have the name
  of the table.

  @details
  When create() is called you do not need to worry about
  opening the table. Also, the .frm file will have already been
  created so adjusting create_info is not necessary. You can overwrite
  the .frm file at this point if you wish to change the table
  definition, but there are no methods currently provided for doing
  so.

  Called from handle.cc by ha_create_table().

  @see
  ha_create_table() in handle.cc
*/

int ha_mysqlite::create(const char *name, TABLE *table_arg,
                       HA_CREATE_INFO *create_info)
{
  DBUG_ENTER("ha_mysqlite::create");
  /*
    This is not implemented but we want someone to be able to see that it
    works.
  */
  DBUG_RETURN(0);
}


struct st_mysql_storage_engine mysqlite_storage_engine=
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

static ulong srv_enum_var= 0;
static ulong srv_ulong_var= 0;

const char *enum_var_names[]=
{
  "e1", "e2", NullS
};

TYPELIB enum_var_typelib=
{
  array_elements(enum_var_names) - 1, "enum_var_typelib",
  enum_var_names, NULL
};

static MYSQL_SYSVAR_ENUM(
  enum_var,                       // name
  srv_enum_var,                   // varname
  PLUGIN_VAR_RQCMDARG,            // opt
  "Sample ENUM system variable.", // comment
  NULL,                           // check
  NULL,                           // update
  0,                              // def
  &enum_var_typelib);             // typelib

static MYSQL_SYSVAR_ULONG(
  ulong_var,
  srv_ulong_var,
  PLUGIN_VAR_RQCMDARG,
  "0..1000",
  NULL,
  NULL,
  8,
  0,
  1000,
  0);

static struct st_mysql_sys_var* mysqlite_system_variables[]= {
  MYSQL_SYSVAR(enum_var),
  MYSQL_SYSVAR(ulong_var),
  NULL
};


bool copy_sqlite_table_formats(/* out */
                               vector<string> &table_names,
                               vector<string> &ddls)
{
  using namespace mysqlite;

  Mysqlite_share *share = Mysqlite_share::get_share();
  my_assert(share->conn.is_opened());

  RowCursor *rows = share->conn.table_fullscan("sqlite_master");
  my_assert(rows);

  while (rows->next()) {
    table_names.push_back(rows->get_text(SQLITE_MASTER_COLNO_NAME));
    ddls.push_back(rows->get_text(SQLITE_MASTER_COLNO_SQL));
  }

  rows->close();
  return true;
}

#ifdef MARIADB
/**
  @brief
  mysqlite_assisted_discovery() is called when creating a table with no columns.

  @details
  When assisted discovery is used the .frm file have not already been
  created. You can overwrite some definitions at this point but the
  main purpose of it is to define the columns for some table types.
*/
static int mysqlite_assisted_discovery(handlerton *hton, THD* thd,
                                       TABLE_SHARE *table_s,
                                       HA_CREATE_INFO *create_info)
{
  int b = 0;
  char        buf[1024];
  PTOS        topt= table_s->option_struct;
  const char *path=   topt->filename;
  bool is_existing_db = false;

  // Open DB and Connection
  Mysqlite_share *share = Mysqlite_share::get_share();
  if (!share) {
    log_errstat(MYSQLITE_OUT_OF_MEMORY);
    return 1;
  }
  errstat res = share->conn.open(path);
  if (res == MYSQLITE_DB_FILE_NOT_FOUND) {
    // Newly create SQLite database file
    is_existing_db = false;
  }
  else if (res == MYSQLITE_OK) {
    // SQLite database file already exists
    is_existing_db = true;
  }
  else {
    log_errstat(res);
    return 1;
  }

  assert(is_existing_db);  // TODO: support new creation of db files
  if (is_existing_db) {
    // Duplicate SQLite DDLs to MySQL
    // TODO: ここで，TABLE_SHARE::table_name に入ってるDDLだけをsqlite_masterから取り出す必要がある
    vector<string> table_names, ddls;
    if (!copy_sqlite_table_formats(table_names, ddls))
      return 1;

    // Create tables
    for (int i = 0; i < table_names.size(); ++i) {
      b = table_s->init_from_sql_statement_string(thd, true,
                                                  ddls[i].c_str(), ddls[i].size());
      if (b) {
        log_msg("Error in creating table: %s\n", ddls[i].c_str());
        return b;
      }
    }
  }
  return 0;
}
#endif //MARIADB

// this is an example of SHOW_FUNC and of my_snprintf() service
static int show_func_mysqlite(MYSQL_THD thd, struct st_mysql_show_var *var,
                             char *buf)
{
  var->type= SHOW_CHAR;
  var->value= buf; // it's of SHOW_VAR_FUNC_BUFF_SIZE bytes
  my_snprintf(buf, SHOW_VAR_FUNC_BUFF_SIZE,
              "enum_var is %lu, ulong_var is %lu, %.6b", // %b is MySQL extension
              srv_enum_var, srv_ulong_var, "really");
  return 0;
}

static struct st_mysql_show_var func_status[]=
{
  {"mysqlite_func_mysqlite",  (char *)show_func_mysqlite, SHOW_FUNC},
  {0,0,SHOW_UNDEF}
};

#ifdef MARIADB
maria_declare_plugin(mysqlite)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &mysqlite_storage_engine,
  "MYSQLITE",
  "Sho Nakatani",
  "MySQLite Storage Engine",
  PLUGIN_LICENSE_GPL,
  mysqlite_init_func,                            /* Plugin Init */
  mysqlite_done_func,                            /* Plugin Deinit */
  MYSQLITE_VERSION,
  func_status,                                   /* status variables */
  mysqlite_system_variables,                     /* system variables */
  "0.0",                      /* string version */
  MariaDB_PLUGIN_MATURITY_EXPERIMENTAL /* maturity */
}
maria_declare_plugin_end;
#else
mysql_declare_plugin(mysqlite)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &mysqlite_storage_engine,
  "MYSQLITE",
  "Sho Nakatani",
  "MySQLite Storage Engine",
  PLUGIN_LICENSE_GPL,
  mysqlite_init_func,                            /* Plugin Init */
  mysqlite_done_func,                            /* Plugin Deinit */
  MYSQLITE_VERSION,
  func_status,                                   /* status variables */
  mysqlite_system_variables,                     /* system variables */
  NULL,                                          /* config options */
  0,
}
mysql_declare_plugin_end;
#endif

#ifndef _UDF_SQLITE_DB_H_
#define _UDF_SQLITE_DB_H_


extern "C" {
  my_bool sqlite_db_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
  void sqlite_db_deinit(UDF_INIT *initid);
  long long sqlite_db(UDF_INIT *initid, UDF_ARGS *args, char *is_null,
                      char *error);
}


#endif /* _UDF_SQLITE_DB_H_ */

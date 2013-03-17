project(mysqlite)
set(mysqlite_VERSION_MAJOR 0)
set(mysqlite_VERSION_MINOR 0)

add_definitions("-DMYSQLITE_VERSION_HEX=0x0001")
add_definitions("-DMYSQLITE_USING_CMAKE")

add_definitions("-DMYSQL_DYNAMIC_PLUGIN")

set(mysqlite_sources src/ha_mysqlite.cc src/udf_sqlite_db.cc)
include_directories(${cmake_source_dir}/storage/mysqlite/src)
mysql_add_plugin(mysqlite ${mysqlite_sources} STORAGE_ENGINE MODULE_ONLY MODULE_OUTPUT_NAME "libmysqlite_engine")

add_library(mysqlite_engine SHARED ${mysqlite_sources})
target_link_libraries(mysqlite_engine ${CMAKE_SOURCE_DIR}/libservices/libmysqlservices.a ${CMAKE_SOURCE_DIR}/libmysql/libmysqlclient.a)

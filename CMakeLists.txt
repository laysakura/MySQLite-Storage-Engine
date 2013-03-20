project(mysqlite)
set(MYSQLITE_VERSION_MAJOR 0)
set(MYSQLITE_VERSION_MINOR 0)


################################################################################
# Directory structure
################################################################################
set(MYSQLITE_TEST_DB_DIR "${CMAKE_CURRENT_SOURCE_DIR}/t/db")


################################################################################
# Define macros
################################################################################
add_definitions("-DMYSQLITE_USING_CMAKE")
add_definitions("-DMYSQL_DYNAMIC_PLUGIN")


################################################################################
# Configure file
################################################################################
configure_file(
  "src/mysqlite_config.h.in"
  "src/mysqlite_config.h"
)


################################################################################
# Compile and link
################################################################################
set(mysqlite_sources src/ha_mysqlite.cc src/udf_sqlite_db.cc src/mysqlite_api.cc)
include_directories(${cmake_source_dir}/storage/mysqlite/src)
mysql_add_plugin(mysqlite ${mysqlite_sources} STORAGE_ENGINE MODULE_ONLY MODULE_OUTPUT_NAME "libmysqlite_engine")

add_library(mysqlite_engine SHARED ${mysqlite_sources})
target_link_libraries(mysqlite_engine ${CMAKE_SOURCE_DIR}/libservices/libmysqlservices.a ${CMAKE_SOURCE_DIR}/libmysql/libmysqlclient.a)

cmake_minimum_required(VERSION 2.8)
project(mysqlite_utest)


################################################################################
# Command line options
################################################################################
option(GTEST_SRC "Path to google test")
if(NOT GTEST_SRC)
  message(fatal_error "Needs -DGTEST_SRC=/path/to/gtest/")
endif()


################################################################################
# Google test
################################################################################
include_directories(${GTEST_SRC} ${GTEST_SRC}/include)
set(GTEST_LDADD ${GTEST_SRC}/src/gtest_main.o ${GTEST_SRC}/src/gtest-all.o -lpthread)


################################################################################
# Unit test executables
################################################################################
set(mysqlite_utest_targets utils pcache_mmap sqlite_format mysqlite_api)


################################################################################
# Compile and link
################################################################################
add_definitions("-std=c++0x")
add_definitions("-fno-exceptions")
add_definitions("-fno-rtti")
add_definitions("-DMYSQLITE_USE_MMAP=1")

include_directories(../../../../sql ../../../../include)


## build test target library (<all sources> - ha_mysqlite.cc)
foreach(t ${mysqlite_utest_targets})
  set(lib_src ${lib_src} ../${t}.cc)
endforeach(t ${mysqlite_utest_targets})
add_library(mysqlite_test_target STATIC ${lib_src})


## build each test executable
foreach(target ${mysqlite_utest_targets})
  add_executable(${target}Test ${target}Test.cc)
  target_link_libraries(${target}Test ${GTEST_LDADD} mysqlite_test_target)
  set_target_properties(
    ${target}Test PROPERTIES COMPILE_FLAGS
    "-g -Wall -Wextra -Wunused -Wwrite-strings -Wno-strict-aliasing -o0 -Werror -Wno-unused-parameter -Woverloaded-virtual"
  )
endforeach(target ${mysqlite_utest_targets})

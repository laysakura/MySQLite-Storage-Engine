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
set(mysqlite_utest_targets utils pcache sqlite_format mysqlite_api)


################################################################################
# Compile and link
################################################################################
include_directories(../../../../sql ../../../../include)

foreach(target ${mysqlite_utest_targets})
  # Add sources necessary to compile each test
  set(utest_src ${target}Test.cc)
  if(EXISTS ${CMAKE_SOURCE_DIR}/../${target}.cc)
    set(utest_src ${utest_src} ${CMAKE_SOURCE_DIR}/../${target}.cc)
  endif()

  # Each test specific source
  if(sqlite_format STREQUAL ${target}
     OR mysqlite_api STREQUAL ${target}
     OR pcache STREQUAL ${target}
  )
    set(utest_src ${utest_src} ../sqlite_format.cc)
  endif()
  if(mysqlite_api STREQUAL ${target}
     OR sqlite_format STREQUAL ${target}
     OR pcache STREQUAL ${target}
  )
    set(utest_src ${utest_src} ../pcache.cc)
  endif()
  if(sqlite_format STREQUAL ${target}
  )
    set(utest_src ${utest_src} ../mysqlite_api.cc)
  endif()

  add_executable(${target}Test ${utest_src})
  target_link_libraries(${target}Test ${GTEST_LDADD})
  set_target_properties(
    ${target}Test PROPERTIES COMPILE_FLAGS
    "-g -Wall -Wextra -Wunused -Wwrite-strings -Wno-strict-aliasing -o0 -Werror -Wno-unused-parameter -Woverloaded-virtual"
  )
endforeach(target ${mysqlite_utest_targets})

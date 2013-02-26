PROJECT(mysqlite_utest)

OPTION(GTEST_SRC "Path to Google Test" )
IF(NOT GTEST_SRC)
  MESSAGE(FATAL_ERROR "Needs -DGTEST_SRC=/path/to/gtest/")
ENDIF()

INCLUDE_DIRECTORIES(${GTEST_SRC} ${GTEST_SRC}/include)
SET(GTEST_LDADD ${GTEST_SRC}/src/gtest_main.o ${GTEST_SRC}/src/gtest-all.o -lpthread)


## Define test executables here
SET(MYSQLITE_UTEST_EXECUTABLES utilsTest sqlite_formatTest)

FOREACH(UTEST ${MYSQLITE_UTEST_EXECUTABLES})
  ADD_EXECUTABLE(${UTEST} ${UTEST}.cc)
  TARGET_LINK_LIBRARIES(${UTEST} ${GTEST_LDADD})
  SET_TARGET_PROPERTIES(${UTEST} PROPERTIES COMPILE_FLAGS "-g -Wall -Wextra -Wunused -Wwrite-strings -Wno-strict-aliasing -O0 -Werror -Wno-unused-parameter -Woverloaded-virtual")
ENDFOREACH(UTEST ${MYSQLITE_UTEST_EXECUTABLES})

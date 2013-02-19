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
ENDFOREACH(UTEST ${MYSQLITE_UTEST_EXECUTABLES})

# add_executable(aaa utilsTest.cc)
# target_link_libraries(aaa  ${GTEST_LDADD})


# Used to build Makefile.in

EXTRA_DIST = ha_mysqlite.h CMakeFiles.txt plug.in mysqlite_probes.d

INCLUDES =   $(MYSQL_INC) -I$(GTEST_SRC) -I$(GTEST_SRC)/include

DTRACE =                @DTRACE@
DTRACEFLAGS =           @DTRACEFLAGS@
DTRACEFILES =           .libs/libmysqlite_engine_la-ha_mysqlite.o

noinst_HEADERS = ha_mysqlite.h \
		 mysqlite_probes.h

lib_LTLIBRARIES = libmysqlite_engine.la
libmysqlite_engine_la_SOURCES = ha_mysqlite.cc udf_sqlite_db.cc
libmysqlite_engine_la_LIBADD = -lmysqlservices $(MYSQL_SRC)/libmysql/libmysqlclient.a

if HAVE_DTRACE
  libmysqlite_engine_la_LIBADD += mysqlite_probes.o
endif

libmysqlite_engine_la_LDFLAGS =	-module -L$(MYSQL_SRC)/libservices/
libmysqlite_engine_la_CFLAGS = $(AM_CFLAGS) -DMYSQL_DYNAMIC_PLUGIN
libmysqlite_engine_la_CXXFLAGS = $(AM_CFLAGS) -DMYSQL_DYNAMIC_PLUGIN


##
# Unit tests (gtest)
##
noinst_PROGRAMS = utilsTest
gtest_ldadd = $(GTEST_SRC)/src/gtest_main.o $(GTEST_SRC)/src/gtest-all.o -lpthread

utilsTest_SOURCES = utilsTest.cc
utilsTest_LDADD = $(gtest_ldadd)


##
# dtrace
##
mysqlite_probes.h: mysqlite_probes.d
	$(DTRACE) $(DTRACEFLAGS) -h -s mysqlite_probes.d
	mv mysqlite_probes.h mysqlite_probes.h.bak
	sed "s/#include <unistd.h>//g" mysqlite_probes.h.bak > mysqlite_probes.h
	rm mysqlite_probes.h.bak

mysqlite_probes.o:
	$(DTRACE) $(DTRACEFLAGS) -G -s mysqlite_probes.d $(DTRACEFILES)

# End

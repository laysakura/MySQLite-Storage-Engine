# Used to build Makefile.in

EXTRA_DIST = ha_mysqlite.h CMakeFiles.txt plug.in mysqlite_probes.d

INCLUDES =   $(MYSQL_INC)

DTRACE =                @DTRACE@
DTRACEFLAGS =           @DTRACEFLAGS@
DTRACEFILES =           .libs/libmysqlite_engine_la-ha_mysqlite.o

noinst_HEADERS = ha_mysqlite.h \
		 mysqlite_probes.h

lib_LTLIBRARIES = libmysqlite_engine.la
libmysqlite_engine_la_SOURCES = ha_mysqlite.cc
libmysqlite_engine_la_LIBADD = $(MYSQL_SRC)/libservices/libmysqlservices.a

if HAVE_DTRACE
  libmysqlite_engine_la_LIBADD += mysqlite_probes.o
endif

libmysqlite_engine_la_LDFLAGS =	-module
libmysqlite_engine_la_CFLAGS = $(AM_CFLAGS) -DMYSQL_DYNAMIC_PLUGIN
libmysqlite_engine_la_CXXFLAGS = $(AM_CFLAGS) -DMYSQL_DYNAMIC_PLUGIN

mysqlite_probes.h: mysqlite_probes.d
	$(DTRACE) $(DTRACEFLAGS) -h -s mysqlite_probes.d
	mv mysqlite_probes.h mysqlite_probes.h.bak
	sed "s/#include <unistd.h>//g" mysqlite_probes.h.bak > mysqlite_probes.h
	rm mysqlite_probes.h.bak

mysqlite_probes.o:
	$(DTRACE) $(DTRACEFLAGS) -G -s mysqlite_probes.d $(DTRACEFILES)

# End

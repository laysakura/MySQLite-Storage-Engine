# Used to build Makefile.in

EXTRA_DIST = ha_skeleton.h CMakeFiles.txt plug.in skeleton_probes.d

INCLUDES =   $(MYSQL_INC)

DTRACE =                @DTRACE@
DTRACEFLAGS =           @DTRACEFLAGS@
DTRACEFILES =           .libs/libskeleton_engine_la-ha_skeleton.o

noinst_HEADERS = ha_skeleton.h \
		 skeleton_probes.h

lib_LTLIBRARIES = libskeleton_engine.la
libskeleton_engine_la_SOURCES = ha_skeleton.cc
libskeleton_engine_la_LIBADD = $(MYSQL_SRC)/libservices/libmysqlservices.a

if HAVE_DTRACE
  libskeleton_engine_la_LIBADD += skeleton_probes.o
endif

libskeleton_engine_la_LDFLAGS =	-module
libskeleton_engine_la_CFLAGS = $(AM_CFLAGS) -DMYSQL_DYNAMIC_PLUGIN
libskeleton_engine_la_CXXFLAGS = $(AM_CFLAGS) -DMYSQL_DYNAMIC_PLUGIN

skeleton_probes.h: skeleton_probes.d
	$(DTRACE) $(DTRACEFLAGS) -h -s skeleton_probes.d
	mv skeleton_probes.h skeleton_probes.h.bak
	sed "s/#include <unistd.h>//g" skeleton_probes.h.bak > skeleton_probes.h
	rm skeleton_probes.h.bak

skeleton_probes.o:
	$(DTRACE) $(DTRACEFLAGS) -G -s skeleton_probes.d $(DTRACEFILES)

# End

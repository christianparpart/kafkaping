# This file is part of the "x0" project, http://github.com/christianparpart/x0>
#   (c) 2009-2017 Christian Parpart <christian@parpart.family>
#
# Licensed under the MIT License (the "License"); you may not use this
# file except in compliance with the License. You may obtain a copy of
# the License at: http://opensource.org/licenses/MIT

EXTRA_DIST = autogen.sh
bin_PROGRAMS =
lib_LIBRARIES =
noinst_LIBRARIES =
check_PROGRAMS =
TESTS =

####### compile opts ##########################################################

AM_CXXFLAGS = \
	$(BASE_CXXFLAGS) \
	$(RDKAFKA_CFLAGS) \
	-I$(top_builddir) \
	-I$(top_srcdir)

AM_LDFLAGS = \
	$(BASE_LIBS) \
	$(PLATFORM_LDFLAGS) \
	$(RDKAFKA_LIBS) \
	-lpthread -ldl

####### kafkaping #############################################################

bin_PROGRAMS += kafkaping

kafkaping_CXXFLAGS = \
	$(AM_CXXFLAGS) \
	-I$(top_srcdir)/src 

kafkaping_LDADD = \
	$(AM_LDFLAGS)

# kafkaping_LDFLAGS += -static -static-libgcc -static-libstdc++

kafkaping_SOURCES = \
	kafkaping.cc

# vim:ts=8
# EOF

AC_PREREQ([2.69])
AC_INIT([kafkaping], [0.1.0], [christian@parpart.family], [], [https://github.com/christianparpart/kafkaping])
dnl AC_CONFIG_AUX_DIR([build-aux])
AC_CONFIG_SRCDIR([configure.ac])
dnl AC_CONFIG_MACRO_DIRS([m4])
AM_INIT_AUTOMAKE([foreign subdir-objects serial-tests])
AM_MAINTAINER_MODE([enable])
AM_SILENT_RULES([yes])

AC_LANG_CPLUSPLUS
AC_PROG_RANLIB
AC_PROG_CC
#AC_PROG_CC_FOR_BUILD
AC_PROG_CXX
#AC_PROG_CXX_FOR_BUILD
#AC_PROC_LIBTOOL
AM_PROG_AR
AM_PROG_AS
#LT_INIT
PKG_PROG_PKG_CONFIG([0.25])

uname_system=`uname -s`
AC_MSG_NOTICE("Operating system: $uname_system")
AM_CONDITIONAL([PLATFORM_LINUX], [test x$uname_system = xLinux])
AM_CONDITIONAL([PLATFORM_DARWIN], [test x$uname_system = xDarwin])

AC_HEADER_STDC

# ----------------------------------------------------------------------------
# some more dependencies

AS_IF([test "x$uname_system" = xDarwin],
      [PLATFORM_CXXFLAGS="-stdlib=libc++"]) 
AS_IF([test "x$uname_system" = xLinux],
      [PLATFORM_LDFLAGS="-lrt"]) 
AC_SUBST([PLATFORM_CXXFLAGS])
AC_SUBST([PLATFORM_LDFLAGS])

RDKAFKA_LIBS="-lrdkafka -lrdkafka++"
AC_SUBST([RDKAFKA_LIBS])

####### OUTPUT ################################################################

BASE_CXXFLAGS="-DXOPEN_SOURCE=600 -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE \
 -D_LARGEFILE_SOURCE -D_LARGE_FILES -Wall -Wextra -std=c++14 \
 -Wno-unused-variable -Wno-unused-parameter \
 $PLATFORM_CXXFLAGS"
AC_SUBST([BASE_CXXFLAGS])
AC_SUBST([BASE_LIBS])

AC_CONFIG_HEADERS(sysconfig.h)
AC_CONFIG_FILES(Makefile)
AC_OUTPUT

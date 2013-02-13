AC_DEFUN([GTEST_SRC_TEST], [
  AC_ARG_WITH(
    [gtest],
    AS_HELP_STRING([--with-gtest],
                   [GoogleTest path (ex: /path/to/gtest-1.6.0)]),
  [
    AC_DEFINE([HAVE_GTEST], [1], [Enables GoogleTest Support])
    GTEST_SRC="$withval"
    AC_SUBST(HAVE_GTEST)
  ],
  [
    AC_MSG_ERROR(["--with-gtest is necessary (to-be-fixed)"])
    GTEST_SRC=""
  ])
])
AM_CONDITIONAL([HAVE_GTEST], [ test -n "$GTEST_SRC" ])

dnl ---------------------------------------------------------------------------
dnl Macro: MYSQL_SRC
dnl ---------------------------------------------------------------------------
AC_DEFUN([MYSQL_SRC_TEST], [
  AC_MSG_CHECKING(for mysql source code)
  AC_ARG_WITH(mysql,
  [[  --with-mysql[=mysql src directory]      
                        Source requir to build engine.]],
  [
    MYSQL_SRC="$withval"
    
    AC_DEFINE([MYSQL_SRC], [1], [Source directory for MySQL])
    MYSQL_INC="-I$withval/sql -I$withval/include -I$withval/regex -I$withval"
    AC_MSG_RESULT(["$withval"])
  ],
  [
    AC_MSG_ERROR(["no mysql source provided"])
  ])
])

dnl ---------------------------------------------------------------------------
dnl Macro: MYSQL_SRC
dnl ---------------------------------------------------------------------------

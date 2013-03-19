#include <gtest/gtest.h>

#include "../mysqlite_api.h"


TEST(TypicalUsage, SmallData)
{
  using namespace mysqlite;

  Connection conn;
  errstat res = conn.open("db/BeerDB-small.sqlite");
  ASSERT_EQ(res, MYSQLITE_OK);

  RowCursor *rows = conn.table_fullscan("Beer");
  ASSERT_TRUE(rows);

  while (rows->next()) {
    ASSERT_EQ(rows->get_type(0), MYSQLITE_TEXT);
    ASSERT_EQ(rows->get_type(1), MYSQLITE_INTEGER);
    int price = rows->get_int(1);
    ASSERT_GE(price, 100);
    ASSERT_LE(price, 1500);
  }

  rows->close();
  conn.close();
}
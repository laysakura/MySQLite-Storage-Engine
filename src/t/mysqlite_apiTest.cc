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
    // Retrieve columns
    ASSERT_EQ(rows->get_type(0), MYSQLITE_TEXT);
    ASSERT_EQ(rows->get_type(1), MYSQLITE_INTEGER);

    int price = rows->get_int(1);
    ASSERT_GE(price, 100);
    ASSERT_LE(price, 1500);
  }

  rows->close();
  conn.close();
}

TEST(CheckAllData, SmallData)
{
  using namespace mysqlite;

  Connection conn;
  errstat res = conn.open("db/BeerDB-small.sqlite");
  ASSERT_EQ(res, MYSQLITE_OK);

  RowCursor *rows = conn.table_fullscan("Beer");
  ASSERT_TRUE(rows);

  { // 1st row
    ASSERT_TRUE(rows->next());
    ASSERT_EQ(rows->get_type(0), MYSQLITE_TEXT);
    ASSERT_EQ(rows->get_type(1), MYSQLITE_INTEGER);

    const char *name = rows->get_text(0);
    ASSERT_STREQ("Shonan Gold", name);

    int price = rows->get_int(1);
    ASSERT_EQ(price, 450);
  }
  { // 2nd row
    ASSERT_TRUE(rows->next());
    ASSERT_EQ(rows->get_type(0), MYSQLITE_TEXT);
    ASSERT_EQ(rows->get_type(1), MYSQLITE_INTEGER);

    const char *name = rows->get_text(0);
    ASSERT_STREQ("Liberty Ale", name);

    int price = rows->get_int(1);
    ASSERT_EQ(price, 500);
  }
  ASSERT_FALSE(rows->next());

  rows->close();
  conn.close();
}

TEST(CheckAllData, SmallData_jp)
{
  using namespace mysqlite;

  Connection conn;
  errstat res = conn.open("db/BeerDB-small-jp.sqlite");
  ASSERT_EQ(res, MYSQLITE_OK);

  RowCursor *rows = conn.table_fullscan("Beer");
  ASSERT_TRUE(rows);

  { // 1st row
    ASSERT_TRUE(rows->next());
    ASSERT_EQ(rows->get_type(0), MYSQLITE_TEXT);
    ASSERT_EQ(rows->get_type(1), MYSQLITE_INTEGER);

    const char *name = rows->get_text(0);
    ASSERT_STREQ("湘南ゴールド", name);

    int price = rows->get_int(1);
    ASSERT_EQ(price, 450);
  }
  { // 2nd row
    ASSERT_TRUE(rows->next());
    ASSERT_EQ(rows->get_type(0), MYSQLITE_TEXT);
    ASSERT_EQ(rows->get_type(1), MYSQLITE_INTEGER);

    const char *name = rows->get_text(0);
    ASSERT_STREQ("リバティーエール", name);

    int price = rows->get_int(1);
    ASSERT_EQ(price, 500);
  }
  ASSERT_FALSE(rows->next());

  rows->close();
  conn.close();
}

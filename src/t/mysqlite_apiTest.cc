#include <gtest/gtest.h>

using namespace std;
#include <string>

#include "../mysqlite_api.h"
#include "../sqlite_format.h"
#include "../mysqlite_config.h"


TEST(Connection, is_opened)
{
  using namespace mysqlite;
  errstat res;
  Connection conn;

  // no connection is opened yet
  ASSERT_FALSE(conn.is_opened());

  // connection to valid db is opend
  res = conn.open(MYSQLITE_TEST_DB_DIR "/BeerDB-small.sqlite");
  ASSERT_EQ(res, MYSQLITE_OK);
  ASSERT_TRUE(conn.is_opened());

  // connection is already opened
  res = conn.open(MYSQLITE_TEST_DB_DIR "/BeerDB-small-jp.sqlite");
  ASSERT_EQ(res, MYSQLITE_CONNECTION_ALREADY_OPEN);
}

TEST(TypicalUsage, SmallData)
{
  using namespace mysqlite;

  Connection conn;
  errstat res = conn.open(MYSQLITE_TEST_DB_DIR "/BeerDB-small.sqlite");
  ASSERT_EQ(res, MYSQLITE_OK);

  conn.rdlock_db();
  std::unique_ptr<RowCursor> rows(conn.table_fullscan("Beer"));
  ASSERT_TRUE(rows);

  while (rows->next()) {
    // Retrieve columns
    ASSERT_EQ(rows->get_type(0), MYSQLITE_TEXT);
    ASSERT_EQ(rows->get_type(1), MYSQLITE_TEXT);
    ASSERT_EQ(rows->get_type(2), MYSQLITE_INTEGER);

    int price = rows->get_int(2);
    ASSERT_GE(price, 100);
    ASSERT_LE(price, 1500);
  }
  conn.unlock_db();
}

TEST(CheckAllData, SmallData)
{
  using namespace mysqlite;

  Connection conn;
  errstat res = conn.open(MYSQLITE_TEST_DB_DIR "/BeerDB-small.sqlite");
  ASSERT_EQ(res, MYSQLITE_OK);

  conn.rdlock_db();
  std::unique_ptr<RowCursor> rows(conn.table_fullscan("Beer"));
  ASSERT_TRUE(rows);

  { // 1st row
    ASSERT_TRUE(rows->next());
    ASSERT_EQ(rows->get_type(0), MYSQLITE_TEXT);
    ASSERT_EQ(rows->get_type(1), MYSQLITE_TEXT);
    ASSERT_EQ(rows->get_type(2), MYSQLITE_INTEGER);

    string name = rows->get_raw_string(1);
    ASSERT_STREQ("Shonan Gold", name.c_str());

    int price = rows->get_int(2);
    ASSERT_EQ(price, 450);
  }
  { // 2nd row
    ASSERT_TRUE(rows->next());
    ASSERT_EQ(rows->get_type(0), MYSQLITE_TEXT);
    ASSERT_EQ(rows->get_type(1), MYSQLITE_TEXT);
    ASSERT_EQ(rows->get_type(2), MYSQLITE_INTEGER);

    string maker = rows->get_raw_string(0);
    ASSERT_STREQ("Sapporo", maker.c_str());
    string name = rows->get_raw_string(1);
    ASSERT_STREQ("Ebisu", name.c_str());

    int price = rows->get_int(2);
    ASSERT_EQ(price, 320);
  }
  { // 3rd row
    ASSERT_TRUE(rows->next());
    ASSERT_EQ(rows->get_type(0), MYSQLITE_TEXT);
    ASSERT_EQ(rows->get_type(1), MYSQLITE_TEXT);
    ASSERT_EQ(rows->get_type(2), MYSQLITE_INTEGER);

    string maker = rows->get_raw_string(0);
    ASSERT_STREQ("Sapporo", maker.c_str());
    string name = rows->get_raw_string(1);
    ASSERT_STREQ("Kuro Label", name.c_str());

    int price = rows->get_int(2);
    ASSERT_EQ(price, 280);
  }
  conn.unlock_db();
}

TEST(TypicalUsage, SmallData_get_blob)
{
  using namespace mysqlite;

  Connection conn;
  errstat res = conn.open(MYSQLITE_TEST_DB_DIR "/BeerDB-small.sqlite");
  ASSERT_EQ(res, MYSQLITE_OK);

  conn.rdlock_db();
  std::unique_ptr<RowCursor> rows(conn.table_fullscan("Beer"));
  ASSERT_TRUE(rows);

  { // 1st row
    ASSERT_TRUE(rows->next());
    ASSERT_EQ(rows->get_type(1), MYSQLITE_TEXT);

    vector<u8> buf;
    rows->get_blob(1, buf);
    string name(reinterpret_cast<const char *>(buf.data()), buf.size());
    ASSERT_STREQ("Shonan Gold", name.c_str());
  }
}

// TEST(CheckAllData, SmallData_jp)
// {
//   using namespace mysqlite;

//   Connection conn;
//   errstat res = conn.open(MYSQLITE_TEST_DB_DIR "/BeerDB-small-jp.sqlite");
//   ASSERT_EQ(res, MYSQLITE_OK);

//   std::unique_ptr<RowCursor> rows(conn.table_fullscan("Beer"));
//   ASSERT_TRUE(rows);

//   { // 1st row
//     ASSERT_TRUE(rows->next());
//     ASSERT_EQ(rows->get_type(0), MYSQLITE_TEXT);
//     ASSERT_EQ(rows->get_type(1), MYSQLITE_INTEGER);

//     const char *name = rows->get_raw_string(0);
//     ASSERT_STREQ("湘南ゴールド", name);

//     int price = rows->get_int(1);
//     ASSERT_EQ(price, 450);
//   }
//   { // 2nd row
//     ASSERT_TRUE(rows->next());
//     ASSERT_EQ(rows->get_type(0), MYSQLITE_TEXT);
//     ASSERT_EQ(rows->get_type(1), MYSQLITE_INTEGER);

//     const char *name = rows->get_raw_string(0);
//     ASSERT_STREQ("リバティーエール", name);

//     int price = rows->get_int(1);
//     ASSERT_EQ(price, 500);
//   }
//   ASSERT_FALSE(rows->next());

//   rows->close();
//   conn.close();
// }

// TEST(OverflowPage, Wikipedia)
// {
//   using namespace mysqlite;

//   Connection conn;
//   errstat res = conn.open(MYSQLITE_TEST_DB_DIR "/wikipedia.sqlite");
//   ASSERT_EQ(res, MYSQLITE_OK);

//   std::unique_ptr<RowCursor> rows(conn.table_fullscan("ICTCompany"));
//   ASSERT_TRUE(rows);

//   { // 1st row
//     rows->next();
//     ASSERT_EQ(rows->get_type(0), MYSQLITE_TEXT);
//     ASSERT_EQ(rows->get_type(1), MYSQLITE_TEXT);
//     ASSERT_STREQ(rows->get_raw_string(0), "http://en.wikipedia.org/wiki/Google");
//     string content(rows->get_raw_string(1));
//     ASSERT_EQ(content.size(), 3395u);
//   }
//   { // 2nd row
//     rows->next();
//     ASSERT_EQ(rows->get_type(0), MYSQLITE_TEXT);
//     ASSERT_EQ(rows->get_type(1), MYSQLITE_TEXT);
//     ASSERT_STREQ(rows->get_raw_string(0), "http://en.wikipedia.org/wiki/Facebook");
//     string content(rows->get_raw_string(1));
//     ASSERT_EQ(content.size(), 3670u);
//   }
//   ASSERT_FALSE(rows->next());

//   rows->close();
//   conn.close();
// }

// TEST(LongerThan2pow16, Wikipedia)
// {
//   using namespace mysqlite;

//   Connection conn;
//   errstat res = conn.open(MYSQLITE_TEST_DB_DIR "/wikipedia.sqlite");
//   ASSERT_EQ(res, MYSQLITE_OK);

//   std::unique_ptr<RowCursor> rows(conn.table_fullscan("Alcohol"));
//   ASSERT_TRUE(rows);

//   { // 1st row, whose content has 66105 bytes (the figure is larger than 2^16 = 65536)
//     rows->next();
//     ASSERT_EQ(rows->get_type(0), MYSQLITE_TEXT);
//     ASSERT_EQ(rows->get_type(1), MYSQLITE_TEXT);
//     ASSERT_STREQ(rows->get_raw_string(0), "http://en.wikipedia.org/wiki/Beer");
//     string content(rows->get_raw_string(1));
//     ASSERT_EQ(content.size(), 66234u);
//   }

//   rows->close();
//   conn.close();
// }

TEST(ST_C0_and_ST_C1, TableLeafPage_int)
{
  using namespace mysqlite;

  Connection conn;
  errstat res = conn.open(MYSQLITE_TEST_DB_DIR "/TableLeafPage-int.sqlite");
  ASSERT_EQ(res, MYSQLITE_OK);

  std::unique_ptr<RowCursor> rows(conn.table_fullscan("t1"));
  ASSERT_TRUE(rows);

  { // 1st row
    rows->next();
    ASSERT_EQ(rows->get_type(0), MYSQLITE_INTEGER);
    ASSERT_EQ(rows->get_int(0), 1);  // This is represented as ST_C1 internally
  }
}

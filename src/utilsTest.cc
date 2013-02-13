#include <gtest/gtest.h>

#include "utils.h"

TEST(varint2u64, 1seq)
{
  varint v;
  v.push_back(7);
  EXPECT_EQ(7, varint2u64(v));
}
TEST(varint2u64, 2seq)
{
  varint v;
  v.push_back(1 + 128);  // 128: MSB=1. So next byte will be read.
  v.push_back(1);
  v.push_back(55);  // This byte is not read since prev byte has MSB=0.
  EXPECT_EQ(128 + 1, varint2u64(v));
}
TEST(varint2u64, 9seq)
{
  varint v;
  v.push_back(128);
  v.push_back(128);
  v.push_back(128);
  v.push_back(128);
  v.push_back(128);
  v.push_back(128);
  v.push_back(128);
  v.push_back(128);
  v.push_back(128);  // 9th byte. Next is not read
  v.push_back(55);
  EXPECT_EQ(128, varint2u64(v));
}
TEST(varint2u64, complex)
{
  varint v;
  v.push_back(1 + 128);
  v.push_back(41);
  EXPECT_EQ(169, varint2u64(v));
}

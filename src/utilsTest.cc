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


TEST(u8s_to_val, to_u8)
{
  u8 seq[100];
  seq[0] = 0xab;
  seq[1] = 0xcd;
  EXPECT_EQ(0xab, u8s_to_val<u8>(&seq[0], 1));
}
TEST(u8s_to_val, to_u16)
{
  u8 seq[100];
  seq[0] = 0x07;
  seq[1] = 0x77;
  EXPECT_EQ(0x777, u8s_to_val<u16>(&seq[0], 2));
}
TEST(u8s_to_val, to_u32)
{
  u8 seq[100];
  seq[0] = 0x12;
  seq[1] = 0x34;
  seq[2] = 0x56;
  seq[3] = 0x78;
  EXPECT_EQ(0x12345678, u8s_to_val<u32>(&seq[0], 4));
}
TEST(u8s_to_val, to_u64)
{
  u8 seq[100];
  seq[0] = 0x12;
  seq[1] = 0x34;
  seq[2] = 0x56;
  seq[3] = 0x78;
  seq[4] = 0x9a;
  seq[5] = 0xbc;
  seq[6] = 0xde;
  seq[7] = 0xf0;
  EXPECT_EQ(0x123456789abcdef0, u8s_to_val<u64>(&seq[0], 8));
}

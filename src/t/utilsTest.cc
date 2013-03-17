#include <gtest/gtest.h>

#include "../utils.h"

TEST(varint2u64, 1seq)
{
  u8 v[100] = {7};
  EXPECT_EQ(7u, varint2u64(v));
}
TEST(varint2u64, 2seq)
{
  u8 v[100] = {
    1 + 128,  // 128: MSB=1. So next byte will be read.
    1,
    55,       // This byte is not read since prev byte has MSB=0.
  };
  EXPECT_EQ(128u + 1u, varint2u64(v));
}
TEST(varint2u64, 9seq)
{
  u8 v[100] = {
    128,
    128,
    128,
    128,
    128,
    128,
    128,
    128,
    128,
    55,  // 9th byte. Next is not read
  };
  EXPECT_EQ(128u, varint2u64(v));
}
TEST(varint2u64, complex)
{
  u8 v[100] = {
    1 + 128,
    41,
  };
  EXPECT_EQ(169u, varint2u64(v));
}
TEST(varint2u64, lenCheck2)
{
  u8 len;
  u8 v[100] = {
    1 + 128,
    41,
  };
  EXPECT_EQ(169u, varint2u64(v, &len));
  EXPECT_EQ(2, len);
}
TEST(varint2u64, lenCheck9)
{
  u8 len;
  u8 v[100] = {
    128,
    128,
    128,
    128,
    128,
    128,
    128,
    128,
    128,
  };
  EXPECT_EQ(128u, varint2u64(v, &len));
  EXPECT_EQ(9, len);
}


TEST(u8s_to_val, to_u8)
{
  u8 seq[100];
  seq[0] = 0xab;
  seq[1] = 0xcd;
  EXPECT_EQ(0xabu, u8s_to_val<u8>(&seq[0], 1));
}
TEST(u8s_to_val, to_u16)
{
  u8 seq[100];
  seq[0] = 0x07;
  seq[1] = 0x77;
  EXPECT_EQ(0x777u, u8s_to_val<u16>(&seq[0], 2));
}
TEST(u8s_to_val, to_u32)
{
  u8 seq[100];
  seq[0] = 0x12;
  seq[1] = 0x34;
  seq[2] = 0x56;
  seq[3] = 0x78;
  EXPECT_EQ(0x12345678u, u8s_to_val<u32>(&seq[0], 4));
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
  EXPECT_EQ(0x123456789abcdef0u, u8s_to_val<u64>(&seq[0], 8));
}

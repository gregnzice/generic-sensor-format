// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "gsfxx.h"

#include <cstdint>

#include <array>
#include <memory>

#include "gtest/gtest.h"

using std::array;
using std::unique_ptr;

namespace gsfxx {
namespace test {
namespace {

// 20070726153705022_0007_20020319_035500p.mb121
TEST(GsfxxRecordHeader, Simple1_10) {
  const uint32_t size = 12;
  array<uint8_t, size> data = {
      {0x47, 0x53, 0x46, 0x2D, 0x76, 0x30, 0x31, 0x2E, 0x31, 0x30, 0x00, 0x00}};
  RecordBuffer buf(reinterpret_cast<void *>(&data), size, RECORD_HEADER);
  unique_ptr<Header> header = Header::Decode(buf);
  ASSERT_NE(nullptr, header);
  EXPECT_EQ(1, header->version_major());
  EXPECT_EQ(10, header->version_minor());
}

TEST(GsfxxRecordHeader, Simple2_9) {
  const uint32_t size = 12;
  array<uint8_t, size> data = {
      {0x47, 0x53, 0x46, 0x2d, 0x76, 0x30, 0x32, 0x2e, 0x30, 0x39, 0x00, 0x00}};
  RecordBuffer buf(reinterpret_cast<void *>(&data), size, RECORD_HEADER);
  unique_ptr<Header> header = Header::Decode(buf);
  ASSERT_NE(nullptr, header);
  EXPECT_EQ(2, header->version_major());
  EXPECT_EQ(9, header->version_minor());
}

TEST(GsfxxRecordHeader, InvalidSizeSmall) {
  const uint32_t size = 11;
  array<uint8_t, size> data = {
      {0x47, 0x53, 0x46, 0x2d, 0x76, 0x30, 0x32, 0x2e, 0x30, 0x39, 0x00}};
  RecordBuffer buf(reinterpret_cast<void *>(&data), size, RECORD_HEADER);
  ASSERT_EQ(nullptr, Header::Decode(buf));
}

TEST(GsfxxRecordHeader, InvalidSizeLarge) {
  const uint32_t size = 13;
  array<uint8_t, size> data = {
      {0x47, 0x53, 0x46, 0x2d, 0x76, 0x30, 0x32, 0x2e, 0x30, 0x39, 0x00, 0x00}};
  RecordBuffer buf(reinterpret_cast<void *>(&data), size, RECORD_HEADER);
  ASSERT_EQ(nullptr, Header::Decode(buf));
}

TEST(GsfxxRecordHeader, InvalidGsfText) {
  const uint32_t size = 12;
  // GSF -> HSF.
  array<uint8_t, size> data = {
      {0x48, 0x53, 0x46, 0x2d, 0x76, 0x30, 0x32, 0x2e, 0x30, 0x39, 0x00, 0x00}};
  RecordBuffer buf(reinterpret_cast<void *>(&data), size, RECORD_HEADER);
  ASSERT_EQ(nullptr, Header::Decode(buf));
}

TEST(GsfxxRecordHeader, InvalidVersionNumber) {
  const uint32_t size = 12;
  // Period replaced with dash: "02-09".
  array<uint8_t, size> data = {
      {0x47, 0x53, 0x46, 0x2d, 0x76, 0x30, 0x32, 0x2d, 0x30, 0x39, 0x00, 0x00}};
  RecordBuffer buf(reinterpret_cast<void *>(&data), size, RECORD_HEADER);
  ASSERT_EQ(nullptr, Header::Decode(buf));
}

TEST(GsfxxRecordComment, Simple) {
  const uint32_t size = 20;
  array<uint8_t, size> data = {{
      0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x05,
      0x61, 0x62, 0x63, 0x64, 0x65, 0x00, 0x00, 0x00,
  }};
  RecordBuffer buf(reinterpret_cast<void *>(&data), size, RECORD_COMMENT);
  unique_ptr<Comment> comment = Comment::Decode(buf);
  ASSERT_NE(nullptr, comment);
  ASSERT_EQ("abcde", comment->comment());
  ASSERT_NEAR(1.000000002, TimePointToSeconds(comment->time_point()), 4e-7);
}

// TODO(schwehr): Why is the rounding error so large?
TEST(GsfxxRecordComment, Time2015July14) {
  const uint32_t size = 20;
  array<uint8_t, size> data = {{
      0x55, 0xA5, 0xD5, 0x4D, 0x3A, 0xDE, 0x68, 0xB1, 0x00, 0x00, 0x00, 0x03,
      0x61, 0x62, 0x63, 0x00,
  }};
  RecordBuffer buf(reinterpret_cast<void *>(&data), size, RECORD_COMMENT);
  unique_ptr<Comment> comment = Comment::Decode(buf);
  ASSERT_NE(nullptr, comment);
  ASSERT_EQ("abc", comment->comment());
  ASSERT_NEAR(1436931405.987654321, TimePointToSeconds(comment->time_point()),
              4e-7);
}

}  // namespace
}  // namespace test
}  // namespace gsfxx

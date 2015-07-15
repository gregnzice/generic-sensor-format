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

#ifndef GSFXX_H_
#define GSFXX_H_

#include <cstdint>

#include <chrono>
#include <memory>
#include <string>

namespace gsfxx {

// TODO(schwehr): Switch to make_unique when C++14 is available on Travis-CI.
template <typename T, typename... Args>
std::unique_ptr<T> MakeUnique(Args &&... args) {
  return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}

enum RecordType {
  RECORD_INVALID = 0,
  RECORD_HEADER = 1,
  RECORD_SWATH_BATHYMETRY_PING = 2,
  RECORD_SOUND_VELOCITY_PROFILE = 3,
  RECORD_PROCESSING_PARAMETERS = 4,
  RECORD_SENSOR_PARAMETERS = 5,
  RECORD_COMMENT = 6,
  RECORD_HISTORY = 7,
  RECORD_NAVIGATION_ERROR = 8,
  RECORD_SWATH_BATHY_SUMMARY = 9,
  RECORD_SINGLE_BEAM_PING = 10,
  RECORD_HV_NAVIGATION_ERROR = 11,
  RECORD_ATTITUDE = 12,
  RECORD_NUM_TYPES,  // Sentinel.
};

extern const char *const RECORD_STRINGS[RECORD_NUM_TYPES];

uint32_t SwapEndian(uint32_t src);

std::chrono::system_clock::time_point SecNsecToTimePoint(int64_t sec, int32_t nsec);
double TimePointToSeconds(std::chrono::system_clock::time_point time_point);

class RecordBuffer {
 public:
  RecordBuffer(const void *buf, uint32_t size, RecordType type)
      : buf_(buf), size_(size), type_(type){};
  uint32_t size() const { return size_; }
  RecordType type() const { return type_; }

  std::string ToString(uint32_t start, uint32_t max_length) const;
  uint32_t ToUnsignedInt32(uint32_t start) const;
 private:
  const void *buf_;
  const uint32_t size_;
  const RecordType type_;
  // TODO(schwehr): mutable Error error_;
};

class FileReaderMmap {
 public:
  FileReaderMmap(const void *data, size_t size)
      : data_(data), offset_(0), size_(size) {}
  static std::unique_ptr<FileReaderMmap> Open(const std::string &filename);
  std::unique_ptr<RecordBuffer> NextBuffer();

 private:
  const void *data_;
  mutable size_t offset_;
  const size_t size_;
};

class Record {};

// Record 1.
class Header : Record {
 public:
  Header(int version_major, int version_minor)
      : version_major_(version_major), version_minor_(version_minor) {}
  static std::unique_ptr<Header> Decode(const RecordBuffer &buf);
  int version_major() { return version_major_; }
  int version_minor() { return version_minor_; }

 private:
  const int version_major_;
  const int version_minor_;
};

// Record 6.
class Comment : Record {
 public:
  Comment(std::chrono::system_clock::time_point time_point, std::string comment)
   : time_point_(time_point), comment_(comment) {};
  static std::unique_ptr<Comment> Decode(const RecordBuffer &buf);
  std::chrono::system_clock::time_point time_point() const { return time_point_; }
  std::string comment() const { return comment_; }
 private:
  const std::chrono::system_clock::time_point time_point_;
  const std::string comment_;
};

}  // namespace gsfxx

#endif  // GSFXX_H_

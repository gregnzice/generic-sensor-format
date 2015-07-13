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

// #include <cstdio>
// #include <cstring>
// #include <dirent.h>
// #include <unistd.h>

#include <iostream>
#include <string>
// #include <vector>

#include "gsfxx.h"

using namespace std;

namespace gsfxx {

int Info(const string &filename) {
  auto file = FileReaderMmap::Open(filename);
  auto buf = file->NextBuffer();
  cout << "record_type: " << buf->type() << "-" << RECORD_STRINGS[buf->type()]
       << " size: " << buf->size() << "\n";
  auto header = Header::Decode(*buf);
  cout << "header: " << header->version_major() << "."
       << header->version_minor() << "\n";
  while ((buf = file->NextBuffer())) {
    cout << "record_type: " << buf->type() << "-" << RECORD_STRINGS[buf->type()]
         << "\n";
  }
  return 0;
}

}  // namespace gsfxx

int main(int /* argc */, char *argv[]) { return gsfxx::Info(argv[1]); }

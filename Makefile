# Copyright 2015 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

VERSION:=${shell cat VERSION}

default:
	@echo
	@echo "        gsf ${VERSION}"
	@echo
	@echo "Build options:"
	@echo
	@echo "  all      - build everything"
	@echo "  clean    - remove all objects and executables"
	@echo
	@echo "  test     - C++ unittests"

test: all
	(cd tests && make $@)

all:
	(cd src && make $@)

clean:
	(cd src && make $@)
	(cd tests && make $@)
	(cd data && make $@)


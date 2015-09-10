# Copyright 2015 Snowflake Computing Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

.PHONY: clean all doc install check_doc check test

all: clean check install

doc:
	R -e 'devtools::document()'

check_doc:
	R -e 'devtools::check_doc()'

check:
	R -e 'devtools::check()' --no-tests

install:
	R CMD INSTALL .

clean:
	-rm -fr man/*
	-rm -f NAMESPACE

test:
	R -e "devtools::test()"

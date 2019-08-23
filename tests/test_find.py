#!/usr/bin/env python
#
# Copyright 2019 Scott Wales
#
# Author: Scott Wales <scott.wales@unimelb.edu.au>
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

from grafanadb.find import find

from fixtures import *

def test_find_single(conn):
    q = find('/short/w35/saw562/scratch')
    r = conn.execute(q)
    assert isinstance(r.fetchone().path, str)

def test_find_list(conn):
    q = find(['/short/w35/saw562/scratch', '/short/w35/saw562/tmp'])
    r = conn.execute(q)
    assert isinstance(r.fetchone().path, str)

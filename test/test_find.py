#!/usr/bin/env python
# Copyright 2019 ARC Centre of Excellence for Climate Extremes
# author: Scott Wales <scott.wales@unimelb.edu.au>
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

from dusql.find import *
from dusql import model

import pytest
import sqlalchemy as sa


def test_find_empty(conn):
    # Empty DB has no files
    q = find(path=None, connection=conn)
    results = conn.execute(q)
    assert len(list(results)) == 0


def test_find_all(conn, sample_db):
    # Find all files in the DB
    q = find(path=None, connection=conn)
    results = conn.execute(q)
    assert len(list(results)) == 4


def test_find_subtree(conn, sample_data, sample_db):
    # Find all files under 'a/c'
    q = find(path=sample_data / 'a' / 'c', connection=conn)
    results = conn.execute(q)
    assert len(list(results)) == 2

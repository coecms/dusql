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


from dusql.scan import scan

from dusql import model
import sqlalchemy as sa

from conftest import count_files


def test_scan_sample(conn, sample_db, sample_data):
    # Check the scanned sample data
    q = sa.select([model.paths.c.inode])
    r = conn.execute(q)

    # Note the root path is not included in the scan
    assert len(list(r)) == count_files(sample_data)


def test_scan_twice(conn, sample_db, sample_data):
    # Scanning the same directory twice should not change data
    q = sa.select([model.paths_fullpath.c.path])
    r = conn.execute(q)
    assert len(list(r)) == count_files(sample_data)

    scan(sample_data, conn)

    q = sa.select([model.paths_fullpath.c.path])
    r = conn.execute(q)
    assert len(list(r)) == count_files(sample_data)

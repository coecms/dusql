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
    q = sa.select([model.paths.c.id])
    r = conn.execute(q)
    assert len(list(r)) == count_files(sample_data)

    scan(sample_data, conn)

    q = sa.select([model.paths.c.id])
    r = conn.execute(q)
    assert len(list(r)) == count_files(sample_data)


def test_scan_root(conn, sample_db, sample_data):
    # Root path should be in the table
    q = sa.select([model.root_paths.c.path])
    r = list(conn.execute(q))
    assert len(r) == 1
    assert r[0].path == str(sample_data)

    # Get the root entry
    q = (sa.select([
        model.paths_fullpath.c.path,
        model.paths.c.parent_inode,
        model.paths.c.inode,
        ])
            .select_from(
                model.paths_fullpath
                .join(model.paths, model.paths.c.id == model.paths_fullpath.c.path_id))
            .where(model.paths.c.inode == sample_data.stat().st_ino))

    r = list(conn.execute(q))

    assert r[0].parent_inode == (sample_data / '..').stat().st_ino
    assert r[0].path == str(sample_data)

    # Parent of 'a' should be the root path
    root_inode = r[0].inode
    q = (sa.select([
            model.paths.c.parent_inode,
            ])
            .where(model.paths.c.inode == (sample_data / 'a').stat().st_ino))
    r = conn.execute(q).scalar()
    assert r == root_inode


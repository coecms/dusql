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
from __future__ import print_function

from dusql.model import *

import sqlalchemy as sa
from conftest import count_files
import os


def test_paths_count(conn, sample_db, sample_data):
    # There should be a paths and paths_fullpath row for each file
    q = sa.select([sa.func.count()]).select_from(paths)
    r = conn.execute(q).scalar()

    assert r == count_files(sample_data)

    q = sa.select([sa.func.count()]).select_from(paths_fullpath)
    r = conn.execute(q).scalar()

    assert r == count_files(sample_data)


def test_paths_parents(conn, sample_db):
    # Each path should have only one parent
    q = (sa.select([paths_parents.c.path_id, sa.func.count().label('count')])
         .where(paths_parents.c.depth == 0)
         .group_by(paths_parents.c.path_id))

    for r in conn.execute(q):
        assert r.count == 1


def test_paths_fullpath(conn, sample_db, sample_data):
    # Make sure paths are correct
    q = sa.select([paths_fullpath.c.path])
    paths = [r.path for r in conn.execute(q)]

    for p, _, fs in os.walk(sample_data):
        assert p in paths

        for f in fs:
            assert os.path.join(p, f) in paths

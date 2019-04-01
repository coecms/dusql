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
from dusql.tags import *


def test_summarise(conn, sample_db, sample_data):
    config = {'paths': [str(sample_data)]}
    t, r = summarise_tag(conn, 'a', config)

    assert t == 'a'
    assert r['size'] > 1
    assert r['inodes'] > 1
    assert r['last seen'] is not None

    config = {}
    t, r = summarise_tag(conn, 'b', config)

    assert r['size'] == 0
    assert r['inodes'] == 0
    assert r['last seen'] is None

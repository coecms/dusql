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

from dusql.config import _construct_config

def test_empty():
    c = _construct_config('')
    assert 'database' in c

def test_tags():
    c = _construct_config("""
    tags:
        umdata:
            paths:
                - /g/data/w35/um
                - /short/w35/cylc-run
            checks:
                group-readable: w35
    """)

    assert 'umdata' in c['tags']
    assert '/g/data/w35/um' in c['tags']['umdata']['paths']

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

import grafanadb.db as db

import pytest
import logging
import os


@pytest.fixture(scope="session")
def db_():
    logging.basicConfig()
    logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
    with db.connect(url=os.environ.get('TEST_DB', db.default_url)) as c:
        yield c


@pytest.fixture
def conn(db_):
    t = db_.begin()
    yield db_
    t.rollback()


@pytest.fixture
def session(db_):
    s = db.Session()
    yield s
    s.rollback()

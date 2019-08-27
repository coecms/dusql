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

from grafanadb.server import *

import pytest

@pytest.fixture
def client():
    app.config['TESTING'] = True
    app.config['DATABASE'] = 'postgresql://localhost:9876/grafana'
    app.config['API_KEY'] = 'test_key'

    with app.test_client() as client:
        yield client

def test_no_key(client):
    r = client.get('/find')

    assert r.status_code == 401

def test_find(client):
    query = {'root_inodes': [(2901541690, 145501262337629518)], 'gid': None, 'not_gid': None, 'uid': None, 'not_uid': None, 'mtime': None, 'size': 17179869184.0, 'api_key': 'test_key'}

    r = client.get('/find', json=query)
    assert r.status_code == 200

    j = r.get_json()

    assert len(j) > 0
    assert isinstance(j[0], str)

def test_du(client):
    query = {'root_inodes': [(2901541690, 145501262337629518)], 'gid': None, 'not_gid': None, 'uid': None, 'not_uid': None, 'mtime': None, 'size': 17179869184.0, 'api_key': 'test_key'}

    r = client.get('/du', json=query)
    assert r.status_code == 200

    j = r.get_json()

    assert j['size'] > 0
    assert j['inodes'] > 0

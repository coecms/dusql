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

from dusql.check import *
from dusql.scan import scan
from dusql.handler import get_path_id
import pytest

def test_directory_group_readable(conn, tmp_path):
    a = tmp_path / 'a'
    a.mkdir()

    # Directory is group readable
    a.chmod(0o740)
    scan(tmp_path, conn)
    root_id = get_path_id(tmp_path, conn)
    r = DirectoryGroupReadable().query([root_id])
    assert len(list(conn.execute(r))) == 0

    # Directory is not group readable
    a.chmod(0o700)
    scan(tmp_path, conn)
    r = DirectoryGroupReadable().query([root_id])
    assert len(list(conn.execute(r))) == 1


@pytest.mark.xfail
def test_netcdf_compression(conn, tmp_path):
    a = tmp_path/'a.nc'
    a.write_text('test')

    scan(tmp_path, conn)
    root_id = get_path_id(tmp_path, conn)

    c = NetCDFCompression(min_size = 0)
    r = c.query([root_id], conn)
    assert len(list(conn.execute(r))) == 1


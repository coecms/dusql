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
import argparse

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


def test_directory_group_readable_cli():
    p = argparse.ArgumentParser()
    s = p.add_subparsers()
    DirectoryGroupReadable.init_parser(s)

    args = p.parse_args('directory-group-readable A'.split())
    c = DirectoryGroupReadable.cli_init(args)

    assert c is not None


def test_owned_by(conn, tmp_path):
    a = tmp_path / 'a'
    a.mkdir()

    b = tmp_path / 'b'
    b.mkdir()

    scan(tmp_path, conn)

    conn.execute("""
        UPDATE paths SET uid = -1
        WHERE id IN (
            SELECT paths.id
            FROM paths
            JOIN basenames ON paths.basename_id = basenames.id
            WHERE basenames.name = 'b'
            )
        """)

    root_id = get_path_id(tmp_path, conn)
    r = OwnedBy(user = -1).query([root_id])

    # Two files not owned by -1 (root and 'a')
    assert len(list(conn.execute(r))) == 2


def test_owned_by_cli():
    p = argparse.ArgumentParser()
    s = p.add_subparsers()
    OwnedBy.init_parser(s)

    args = p.parse_args('owned-by A --user root --group root'.split())
    c = OwnedBy.cli_init(args)

    assert c is not None


@pytest.mark.xfail
def test_netcdf_compression(conn, tmp_path):
    a = tmp_path/'a.nc'
    a.write_text('test')

    scan(tmp_path, conn)
    root_id = get_path_id(tmp_path, conn)

    c = NetCDFCompression(min_size = 0)
    r = c.query([root_id], conn)
    assert len(list(conn.execute(r))) == 1


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

from dusql.handler.mdss import *

import io
import stat
import os

import pytest


def test_parse_mdss():

    output = """saw562:
total 4
1511828541626 drwxrwsr-x   5 5424    5608             119 2019-01-24 11:43 (REG) .
1578400487646 drwxrws---  11 0       5608             163 2019-02-04 10:30 (REG) ..
1511828760656 -rw-r--r--   1 5424    5608       315973404 2016-06-22 14:23 (OFL) historical.pm-1850004001.nc
1511832938642 -rw-r--r--   1 5424    5608    327483043840 2018-03-05 11:17 (OFL) um-ostia.tar
1513975992681 drwxrws---   2 5424    5608              10 2016-06-10 11:38 (REG) vapjv
1733019332892 drwxrws---   2 5424    5608              10 2016-06-16 11:38 (REG) vapjw
1664309309747 drwxrws---   2 5424    5608              10 2016-10-07 11:22 (REG) vapjy

"""

    stream = io.StringIO(output)

    r = list(parse_mdss(stream))

    assert r[1]['inode'] == 1511828760656
    assert r[1]['parent_inode'] == 1511828541626
    assert r[1]['uid'] == 5424
    assert r[1]['gid'] == 5608
    assert r[1]['size'] == 315973404


def test_mode_to_octal():
    mode = 'drwxrwsr-x'
    omode = mode_to_octal(mode)
    assert stat.filemode(omode) == mode

    mode = '-rw-r--r--'
    omode = mode_to_octal(mode)
    assert stat.filemode(omode) == mode

    mode = 'lrwxrwxrwx'
    omode = mode_to_octal(mode)
    assert stat.filemode(omode) == mode

    mode = '---s--x---'
    omode = mode_to_octal(mode)
    assert stat.filemode(omode) == mode


@pytest.mark.skipif(not os.environ['HOSTNAME'].startswith('raijin'),
        reason="Only available at NCI")
def test_scan_mdss(conn):
    from dusql.scan import scan
    scan('mdss://w35/saw562', conn)

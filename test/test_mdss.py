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

from dusql.mdss import *

import io
import stat

def test_parse_mdss():

    output = """saw562:
total 4
1511828541626 drwxrwsr-x   5 5424    5608             119 2019-01-24 11:43 (REG) .
1578400487646 drwxrws---  11 0       5608             163 2019-02-04 10:30 (REG) ..
1511828760656 -rw-r--r--   1 5424    5608       315973404 2016-06-22 14:23 (OFL) historical.pm-1850004001.nc
1511832938642 -rw-r--r--   1 5424    5608    327483043840 2018-03-05 11:17 (OFL) um-ostia.tar
"""

    stream = io.StringIO(output)

    for r in parse_mdss(stream):
        print(r)

    assert False


def test_mode_to_octal():
    mode = 'drwxrwsr-x'
    omode = mode_to_octal(mode)

    assert stat.filemode(omode) == mode

    mode = '-rw-r--r--'
    omode = mode_to_octal(mode)

    assert stat.filemode(omode) == mode

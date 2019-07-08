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

from . import model as m

import sqlalchemy as sa
import stat

def unreadable_dirs_query():
    """
    Returns (uid, path) for each directory that is not group readable
    """
    q = (sa.select([m.inode.uid, m.dusql_path_func(m.inode.id).alias('path')])
            .where((m.inode.mode & stat.S_IFDIR) != 0) 
            .where((m.inode.mode & (stat.S_IRGRP | stat.s_IXGRP)) != 0) 
            .order_by(m.inode.uid))

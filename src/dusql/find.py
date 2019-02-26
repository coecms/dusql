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

from . import model

import sqlalchemy as sa
import pandas
import pwd
import grp
import os


def find(path, connection, older_than=None, user=None, group=None):
    j = sa.sql.join(model.paths, model.paths_closure, model.paths.c.parent_inode == model.paths_closure.c.id)
    q = sa.sql.select([
        model.paths.c.name,
        ]).select_from(j)
    
    if path is not None:
        path_inode = os.stat(path).st_ino
        q = q.where(model.paths_closure.c.root == path_inode)

    if older_than is not None:
        delta = pandas.to_timedelta(older_than)
        ts = (pandas.Timestamp.now(tz='UTC') - delta)
        ts = ts.timestamp()
        q = q.where(model.paths.c.mtime < ts)

    if user is not None:
        q = q.where(model.paths.c.uid == pwd.getpwnam(user).pw_uid)

    if group is not None:
        q = q.where(model.paths.c.uid == grp.getgrnam(group).gr_gid)

    for r in connection.execute(q):
        print(r.name)

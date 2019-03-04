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
from .scan import autoscan

import sqlalchemy as sa
import pandas
import pwd
import grp
import os


def find(path, connection, older_than=None, user=None, group=None, exclude=None, size=None):
    autoscan(path, connection)

    j = (model.paths_fullpath
        .join(model.paths, model.paths.c.id == model.paths_fullpath.c.path_id))

    q = (sa.sql.select([
            model.paths_fullpath.c.path,
        ])
        .select_from(j)
        )

    if path is not None:
        path_inode = os.stat(path).st_ino
        parent_path = sa.alias(model.paths)

        j = (j.join(model.paths_parents, model.paths.c.id == model.paths_parents.c.path_id)
                .join(parent_path, parent_path.c.id == model.paths_parents.c.parent_id))

        q = q.select_from(j).where(parent_path.c.inode == path_inode)

    if older_than is not None:
        ts = (pandas.Timestamp.now(tz='UTC') - older_than)
        ts = ts.timestamp()
        q = q.where(model.paths.c.mtime < ts)

    if user is not None:
        q = q.where(model.paths.c.uid.in_([pwd.getpwnam(u).pw_uid for u in user]))

    if group is not None:
        q = q.where(model.paths.c.uid.in_([grp.getgrnam(g).gr_gid for g in group]))

    if exclude is not None:
        excl_q = (sa.select([model.paths_parents.c.path_id])
                .select_from(model.paths_parents
                    .join(model.paths, model.paths.c.id == model.paths_parents.c.parent_id))
                .where(model.paths.c.name.in_(exclude)))
        q = q.where(~model.paths.c.id.in_(excl_q))

    if size is not None:
        if size > 0:
            q = q.where(model.paths.c.size >= size)
        else:
            q = q.where(model.paths.c.size < -size)

    return q

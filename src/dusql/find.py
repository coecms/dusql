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

# SELECT full_path.path
# FROM (
#   SELECT
#     expanded_parent.inode AS inode,
#     group_concat(expanded_parent.name, '/') AS path
#   FROM (
#     SELECT
#       target_path.inode AS inode,
#       paths_closure.depth AS depth,
#       parent_path.name AS name
#     FROM paths AS target_path
#     JOIN paths_closure ON target_path.inode = paths_closure.root
#     JOIN paths AS parent_path ON parent_path.inode = paths_closure.id
#     WHERE paths_closure.idcolumn = 'parent_inode'
#       AND paths_closure.parentcolumn = 'inode'
#     ORDER BY target_path.inode, paths_closure.depth DESC
#   ) AS expanded_parent
#   GROUP BY expanded_parent.inode
# ) AS full_path


def find(path, connection, older_than=None, user=None, group=None):
    autoscan(path, connection)

    target_path = sa.sql.alias(model.paths, 'target_path')
    parent_path = sa.sql.alias(model.paths, 'parent_path')
    parent_closure = sa.sql.alias(model.paths_closure, 'parent_closure')

    # Create a view 'path.id','depth','parent_path.name' to construct full paths
    j = (sa.sql.join(target_path, parent_closure, target_path.c.parent_inode == parent_closure.c.root)
            .join(parent_path, parent_path.c.inode == parent_closure.c.id))
    q = (sa.sql.select([
            target_path.c.id,
            target_path.c.name,
            parent_closure.c.depth,
            parent_path.c.name.label('path_fragment'),
        ])
        .select_from(j)
        .where(parent_closure.c.idcolumn == 'parent_inode')
        .where(parent_closure.c.parentcolumn == 'inode')
        .order_by(target_path.c.inode, parent_closure.c.depth.desc())
        .alias('expanded_parent'))

    # Group all the parent path components into a file path for each target
    full_path = (sa.sql.select([
          q.c.id,
          (sa.sql.expression.func.group_concat(q.c.path_fragment,'/') + '/' + q.c.name).label('path'),
        ])
        .select_from(q)
        .group_by(q.c.id)
        .alias('full_path'))

    # Join the full paths back to the path table for further filtering
    j = sa.sql.join(model.paths, full_path, model.paths.c.id == full_path.c.id)
    q = sa.sql.select([
        full_path.c.path,
        ]).select_from(j)

    if path is not None:
        path_inode = os.stat(path).st_ino
        j = sa.sql.join(j, model.paths_closure, model.paths.c.parent_inode == model.paths_closure.c.id)
        q = q.select_from(j).where(model.paths_closure.c.root == path_inode)

    if older_than is not None:
        delta = pandas.to_timedelta(older_than)
        ts = (pandas.Timestamp.now(tz='UTC') - delta)
        ts = ts.timestamp()
        q = q.where(model.paths.c.mtime < ts)

    if user is not None:
        q = q.where(model.paths.c.uid == pwd.getpwnam(user).pw_uid)

    if group is not None:
        q = q.where(model.paths.c.uid == grp.getgrnam(group).gr_gid)

    return q

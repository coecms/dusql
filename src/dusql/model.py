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

"""
SQL Model
=========

The model is based off of one main table, ``paths``, and a number of views for
ease of use.

The ``paths`` table represents inodes on the file system, and consists of a
number columns representing os.stat() values as well as the column
``parent_inode`` which is the inode of the directory containing this file.

Due to hard links the value of ``inode`` is not globally unique.

The ``paths_closure`` virtual table uses the sqlite closure extension to
calculate the structure of the filesystem tree, based on the ``inode`` and
``parent_inode`` columns of the ``paths`` table.

The ``paths_parents`` and ``paths_descendants`` views convert the ``inode``
dependencies convert the inode-based information in ``paths_closure`` into row
ids of the ``paths`` table.

The ``paths_fullpath`` view contains the full path to a file

To use the views within sqlite the closure extension must be loaded, e.g. with::

    .load src/dusql/closure
"""

from __future__ import print_function

from .closure_ext import closure_table
from .view_ext import view
import sqlalchemy as sa

metadata = sa.MetaData()

#: Inode information from scanning the filesystem
paths = sa.Table('paths', metadata,
        sa.Column('id',sa.Integer,primary_key=True),
        sa.Column('name',sa.String),
        sa.Column('inode',sa.Integer,index=True),
        sa.Column('size',sa.Integer),
        sa.Column('mtime',sa.Float),
        sa.Column('ctime',sa.Float),
        sa.Column('parent_inode',sa.Integer,sa.ForeignKey('paths.inode'),index=True),
        sa.Column('uid', sa.Integer),
        sa.Column('gid', sa.Integer),
        sa.Column('mode', sa.Integer),
        sa.Column('device', sa.Integer),
        sa.Column('last_seen',sa.Float),
        sa.Column('mdss_state',sa.String),
        sa.Column('links',sa.String),
        sa.UniqueConstraint('inode','parent_inode','name',name='uniq_inode_edge'),
        )

_other_paths = sa.sql.alias(paths)

paths_parent_id = view('paths_parent_id', metadata,
        sa.sql.select([paths.c.id.label('id'), _other_paths.c.id.label('parent_id')])
        .select_from(
            paths
            .join(_other_paths, _other_paths.c.inode == paths.c.parent_inode)
            ))


#: Virtual table describing the tree structure
paths_closure = closure_table('paths_closure', metadata,
        tablename='paths_parent_id',
        idcolumn='id',
        parentcolumn='parent_id')


#: Descendents of a given inode
paths_descendants = view('paths_descendants', metadata,
        sa.sql.select([
            paths.c.id.label('path_id'),
            paths_closure.c.depth.label('depth'),
            paths_closure.c.id.label('desc_id'),
            ])
        .select_from(
            paths
            .join(paths_closure, paths.c.id == paths_closure.c.root)
        )
        )


#: Parents of a given inode
paths_parents = view('paths_parents', metadata,
        sa.sql.select([
            paths.c.id.label('path_id'),
            paths_closure.c.depth.label('depth'),
            paths_closure.c.id.label('parent_id'),
            ])
        .select_from(
            paths
            .join(paths_closure, paths.c.id == paths_closure.c.root)
        )
        .where(paths_closure.c.idcolumn == 'parent_id')
        .where(paths_closure.c.parentcolumn == 'id')
        )


# Subquery to order results correctly for GROUP_CONCAT
_q = (sa.sql.select([
        paths_parents.c.path_id,
        paths_parents.c.depth,
        paths.c.name,
        ])
        .select_from(
            paths
            .join(paths_parents, paths.c.id == paths_parents.c.parent_id)
        )
        .order_by(paths_parents.c.path_id, paths_parents.c.depth.desc())
        )


#: Full paths to an inode
paths_fullpath = view('paths_fullpath', metadata,
        sa.sql.select([
            _q.c.path_id.label('path_id'),
            sa.sql.expression.func.group_concat(_q.c.name,'/').label('path')
            ])
        .select_from(_q)
        .group_by(_q.c.path_id)
        )

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

import sqlalchemy as sa

metadata = sa.MetaData()

#: Inode information from scanning the filesystem
paths_ingest = sa.Table('paths_ingest', metadata,
        sa.Column('id',sa.Integer,primary_key=True),
        sa.Column('name',sa.String),
        sa.Column('inode',sa.Integer),
        sa.Column('device', sa.Integer),

        sa.Column('parent_inode',sa.Integer),
        sa.Column('parent_device',sa.Integer),

        sa.Column('size',sa.Integer),
        sa.Column('mtime',sa.Float),
        sa.Column('ctime',sa.Float),
        sa.Column('uid', sa.Integer),
        sa.Column('gid', sa.Integer),
        sa.Column('mode', sa.Integer),
        sa.Column('last_seen',sa.Float),
        sa.Column('mdss_state',sa.String),
        sa.Column('links',sa.Integer),
        )

basenames = sa.Table('basenames', metadata,
        sa.Column('id',sa.Integer,primary_key=True),
        sa.Column('name',sa.Text),
        )

paths = sa.Table('paths', metadata,
        sa.Column('id',sa.Integer,primary_key=True),
        sa.Column('parent_id',sa.Integer, sa.ForeignKey('paths.id')),
        sa.Column('basename_id',sa.Integer, sa.ForeignKey('basenames.id')),

        sa.Column('inode',sa.Integer),
        sa.Column('device', sa.Integer),

        sa.Column('parent_inode',sa.Integer),
        sa.Column('parent_device',sa.Integer),

        sa.Column('size',sa.Integer),
        sa.Column('mtime',sa.Float),
        sa.Column('ctime',sa.Float),
        sa.Column('uid', sa.Integer),
        sa.Column('gid', sa.Integer),
        sa.Column('mode', sa.Integer),
        sa.Column('last_seen',sa.Float),
        sa.Column('links',sa.Integer),
        )

#: Virtual table describing the tree structure
paths_closure = sa.Table('paths_closure', metadata,
        sa.Column('root', sa.Integer, sa.ForeignKey('paths.id')),
        sa.Column('id', sa.Integer, sa.ForeignKey('paths.id')),
        sa.Column('depth', sa.Integer),
        sa.Column('idcolumn', sa.Text),
        sa.Column('parentcolumn', sa.Text),
        )


#: Descendents of a given inode
paths_descendants = sa.Table('paths_descendants', metadata,
        sa.Column('path_id', sa.Integer, sa.ForeignKey('paths.id')),
        sa.Column('desc_id', sa.Integer, sa.ForeignKey('paths.id')),
        sa.Column('depth', sa.Integer),
        )


#: Parents of a given inode
paths_parents = sa.Table('paths_parents', metadata,
        sa.Column('path_id', sa.Integer, sa.ForeignKey('paths.id')),
        sa.Column('parent_id', sa.Integer, sa.ForeignKey('paths.id')),
        sa.Column('depth', sa.Integer),
        )


#: Full paths to an inode
paths_fullpath = sa.Table('paths_fullpath', metadata,
        sa.Column('path_id', sa.Integer, sa.ForeignKey('paths.id')),
        sa.Column('path', sa.Text),
        )

#: Root directory of scans, for finding absolute paths
root_paths = sa.Table('root_paths', metadata,
        sa.Column('path_id', sa.Integer, sa.ForeignKey('paths.id')),
        sa.Column('path', sa.Text),
        )

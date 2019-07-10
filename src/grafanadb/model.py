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

import sqlalchemy as sa

metadata = sa.MetaData()

# An inode on the file system
inode = sa.Table('dusql_inode', metadata,
                 sa.Column('id', sa.Integer, primary_key=True),
                 sa.Column('basename', sa.Text),
                 sa.Column('parent_device', sa.BigInteger),
                 sa.Column('parent_inode', sa.BigInteger),
                 sa.Column('device', sa.BigInteger),
                 sa.Column('inode', sa.BigInteger),
                 sa.Column('mode', sa.Integer),
                 sa.Column('uid', sa.Integer),
                 sa.Column('gid', sa.Integer),
                 sa.Column('size', sa.BigInteger),
                 sa.Column('mtime', sa.Float),
                 sa.Column('scan_time', sa.Float),
                 )

# Link an inode to its parent
parent = sa.Table('dusql_parent', metadata,
                  sa.Column('id', sa.Integer, sa.ForeignKey('dusql_inode.id'), primary_key=True),
                  sa.Column('parent_id', sa.Integer, sa.ForeignKey('dusql_inode.id')),
                  )

# Pass a inode id to dusql_path_func inside a query to get the full path to that inode
dusql_path_func = sa.func.dusql_path_func

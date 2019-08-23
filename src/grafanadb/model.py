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
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy.orm as orm

metadata = sa.MetaData()
Base = declarative_base()

# An inode on the file system
class Inode(Base):
    __tablename__ = 'dusql_inode'

    basename = sa.Column('basename', sa.Text, primary_key=True)
    inode = sa.Column('inode', sa.BigInteger)
    device = sa.Column('device', sa.BigInteger, primary_key=True)
    mode = sa.Column('mode', sa.Integer)
    uid = sa.Column('uid', sa.Integer)
    gid = sa.Column('gid', sa.Integer)
    size = sa.Column('size', sa.BigInteger)
    mtime = sa.Column('mtime', sa.Float)
    scan_time = sa.Column('scan_time', sa.Float)
    root_inode = sa.Column('root_inode', sa.BigInteger, sa.ForeignKey('dusql_inode.inode'))
    parent_inode = sa.Column('parent_inode', sa.BigInteger, sa.ForeignKey('dusql_inode.inode'), primary_key=True)

    root = orm.relationship('Inode', primaryjoin=sa.and_(orm.remote(inode)==orm.foreign(root_inode), orm.remote(device) == orm.foreign(device)))
    parent = orm.relationship('Inode', primaryjoin=sa.and_(orm.remote(inode)==orm.foreign(parent_inode), orm.remote(device) == orm.foreign(device)))

    path = orm.column_property(sa.func.dusql_path_func(parent_inode, device, basename), deferred=True)

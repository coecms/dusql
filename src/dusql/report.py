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
import sqlalchemy.sql.functions as safunc
import pwd
import grp
import pandas

def report(path, connection):
    total = (sa.sql.select([
        model.paths.c.uid,
        model.paths.c.gid,
        safunc.sum(model.paths.c.size).label('size'),
        safunc.count().label('inodes')])
        .group_by(model.paths.c.uid, model.paths.c.gid))

    df = pandas.read_sql(total, connection)

    df['name'] = df['uid'].apply(lambda u: pwd.getpwuid(u).pw_gecos)
    df['group'] = df['gid'].apply(lambda u: grp.getgrgid(u).gr_name)

    df['total size (GB)'] = df['size']/1024**3

    print(df[['name','group','total size (GB)','inodes']])

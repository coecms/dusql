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

from grafanadb import model as m

import sqlalchemy as sa
import pwd

def unreadable_query():
    """
    Returns (uid, path) for each path that cannot be 'stat'ed or directory opened
    """
    pinode = m.inode.alias('parent_inode')

    q = (sa.select([pinode])
            .select_from(
                pinode
                .join(m.parent, m.parent.c.parent_id == pinode.c.id)
                .join(m.inode, m.parent.c.id == m.inode.c.id)
            )
            .where(m.inode.c.mode == None)
            .where(sa.not_( m.dusql_path_func(m.inode.c.id).like('%/tmp/%')))
            .alias('unreadable')
            )

    return q

def unreadable_report(conn):
    # Count by user
    uq = unreadable_query()

    q1 = sa.select([uq.c.uid, sa.func.count().label('count')]).group_by(uq.c.uid)

    for user in conn.execute(q1):
        if user is None:
            continue

        p = pwd.getpwuid(user.uid)
        username = p.pw_name
        fullname = p.pw_gecos

        print(f'{fullname} ({username}): {user.count}')

        with open(f'/g/data/w35/saw562/dusql/users/{username}.txt', 'w') as f:
            q2 = sa.select([m.dusql_path_func(uq.c.id).label('path')]).where(uq.c.uid == user.uid)
            for path in conn.execute(q2):
                f.write(path.path + '\n')


if __name__ == '__main__':
    from grafanadb import db

    with db.connect() as conn:
        unreadable_report(conn)

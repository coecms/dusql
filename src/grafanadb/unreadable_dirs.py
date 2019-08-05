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
import pandas
import sys
import json
from jinja2 import Template

email_template = """
Hi {{fullname}},

You've got {{count}} {{'directories' if count > 1 else 'directory'}} using CLEX storage at NCI that CMS can't see.

{% if count < 5 -%}
Please review the paths listed below to ensure no data with access restrictions is included in those paths. 

If no such data exists, please fix your permissions as soon as possible by running:
{% for path in paths %}
    chmod -R g+rX {{path}}
{%- endfor -%}
{%- else -%}
Please review the paths listed in /g/data/w35/saw562/dusql/users/{{username}}.txt to ensure no data with access restrictions is included in those paths. 

If no such data exists, please fix your permissions as soon as possible by running:

    xargs chmod -R g+rX < /g/data/w35/saw562/dusql/users/{{username}}.txt
{%- endif %}

If you have data you can not open to the whole group, please contact the CMS team (cws_help@nci.org.au) with the details of which paths need to keep access restrictions and why.

Tip: remember to check this wiki page [1] to find ways to create files with group permissions directly.

Sincerely, CLEX CMS

[1] http://climate-cms.wikis.unsw.edu.au/Tips:_Custom_file_permissions_at_creation
"""

def unreadable_query():
    """
    Returns (uid, path) for each path that cannot be 'stat'ed or directory opened
    """
    pinode = m.inode.alias('parent_inode')

    q = (sa.select([pinode.c.uid, pinode.c.gid, m.dusql_path_func(pinode.c.id).label('path'), m.dusql_project_func(pinode.c.id).label('root_gid')])
            .select_from(
                pinode
                .join(m.parent, pinode.c.id == m.parent.c.parent_id)
                .join(m.inode, m.inode.c.id == m.parent.c.id)
                )
            .where(m.inode.c.mode == None)
            ).alias('unreadable')

    q = (sa.select([q])
            .where(sa.not_(q.c.path.like('%/tmp/%')))
            .where(sa.not_(q.c.path.like('%/dask-worker-space/%')))
            .where(q.c.gid == q.c.root_gid)
            )

    return q

def unreadable_report(conn):
    # Count by user
    uq = unreadable_query()

    df = pandas.read_sql_query(uq, conn)

    j_template = Template(email_template)

    df = df[df.uid.isin([11364, 6826,5424,603, 2014])]

    messages = []

    for uid, group in df.groupby('uid'):

        p = pwd.getpwuid(uid)
        username = p.pw_name
        fullname = p.pw_gecos

        paths = group['path']

        messages.append({
            'to': f'{fullname} <{username}@nci.org.au>',
            'subject': f'CLEX storage for {username}',
            'message': j_template.render(username = p.pw_name, fullname = p.pw_gecos, count = paths.size, paths = paths)
            })
        print(messages[-1]['message'])

        with open(f'/g/data/w35/saw562/dusql/users/{username}.txt', 'w') as f:
            for path in paths:
                f.write(path + '\n')

    json.dump(messages, sys.stdout, indent=4)


if __name__ == '__main__':
    from grafanadb import db

    with db.connect() as conn:
        unreadable_report(conn)

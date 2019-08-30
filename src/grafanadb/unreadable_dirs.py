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

As per Claire's email on the 12/08/2019 CLEX CMS is now monitoring storage usage for our projects

You've got {{count}} {{'directories' if count > 1 else 'directory'}} using CLEX storage at NCI that we can't see.

{% if count < 5 -%}
Please review the paths listed below to ensure no data with access restrictions is included in those paths. 

If no such data exists, please fix your permissions as soon as possible by running:
{% for path in paths %}
    chmod -R g+rX "{{path}}"
{%- endfor -%}
{%- else -%}
Please review the paths listed in /g/data/hh5/tmp/dusql/users/{{username}}.txt to ensure no data with access restrictions is included in those paths. 

If no such data exists, please fix your permissions as soon as possible by running:

    xargs chmod -R g+rX < /g/data/hh5/tmp/dusql/users/{{username}}.txt
{%- endif %}

If you have data you can not open to the whole group, please contact the CMS team (cws_help@nci.org.au) with the details of which paths need to keep access restrictions and why.

You can check out our prototype monitoring dashboard at [1] (log in with your NCI account), and information on our 'dusql' tool for looking at storage use at [2]. Please let us know if you have any suggestions on what it would be useful to measure.  

Tip: remember to check this wiki page [3] to find ways to create files with group permissions directly.

Sincerely, CLEX CMS

[1] https://accessdev.nci.org.au/grafana/d/toeLAYDWz/user-report?orgId=1&var-userid={{username}}
[2] http://climate-cms.wikis.unsw.edu.au/Dusql
[3] http://climate-cms.wikis.unsw.edu.au/Tips:_Custom_file_permissions_at_creation
"""


def unreadable_query():
    """
    Returns (uid, path) for each path that cannot be 'stat'ed or directory opened
    """
    parent = m.Inode.__table__.alias("parent")
    root = m.Inode.__table__.alias("root")
    inode = m.Inode.__table__.alias("inode")

    subq = (
        sa.select(
            [
                parent.c.uid,
                parent.c.gid,
                root.c.gid.label("root_gid"),
                parent.c.basename,
                parent.c.device,
                parent.c.parent_inode,
            ]
        )
        .select_from(
            inode.join(
                parent,
                sa.and_(
                    inode.c.parent_inode == parent.c.inode,
                    inode.c.device == parent.c.device,
                ),
            ).join(
                root,
                sa.and_(
                    inode.c.root_inode == root.c.inode, inode.c.device == root.c.device
                ),
            )
        )
        .where(inode.c.mode == None)
        .distinct()
        .alias()
    )

    q = sa.select(
        [
            subq.c.uid,
            subq.c.gid,
            subq.c.root_gid,
            sa.func.dusql_path_func(
                subq.c.parent_inode, subq.c.device, subq.c.basename
            ).label("path"),
        ]
    ).alias()
    q = sa.select(q.c).where(sa.not_(q.c.path.like("%/tmp/%")))

    return q


def unreadable_report(conn):
    # Count by user
    uq = unreadable_query()

    df = pandas.read_sql_query(uq, conn)
    print(df)

    j_template = Template(email_template)

    messages = []

    for uid, group in df.groupby("uid"):

        p = pwd.getpwuid(uid)
        username = p.pw_name
        fullname = p.pw_gecos

        paths = group["path"]

        messages.append(
            {
                "to": f"{fullname} <{username}@nci.org.au>",
                "subject": f"CLEX storage at NCI for {username}",
                "message": j_template.render(
                    username=p.pw_name,
                    fullname=p.pw_gecos,
                    count=paths.size,
                    paths=paths,
                ),
            }
        )
        print(messages[-1]["message"])

        with open(f"/g/data/hh5/tmp/dusql/users/{username}.txt", "w") as f:
            for path in paths:
                f.write(path + "\n")

    json.dump(messages, sys.stdout, indent=4)


if __name__ == "__main__":
    from grafanadb import db

    with db.connect() as conn:
        unreadable_report(conn)

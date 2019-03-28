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
from .handler import get_path_id
from . import __version__

import sqlalchemy as sa
import pandas
import pwd
import grp
import stat
from datetime import datetime


def find_children(root_ids):
    """
    Get all child ids of the paths identified by root_ids

    Args:
        root_ids: list of model.path.c.id

    Returns:
        sa.select of model.paths.c.id
    """

    q = (
        sa.select([
            model.paths.c.id,
        ])
        .select_from(
            model.paths
            .join(
                model.paths_closure,
                model.paths.c.id == model.paths_closure.c.id)
            )
        .where(model.paths_closure.c.root.in_(root_ids))
        .distinct()
        )
    return q


def find_roots():
    """
    Find all root path ids

    Returns:
        sa.select of model.paths.c.id
    """
    q = (
        sa.select([
            model.paths_fullpath.c.path_id.label('id'),
            model.paths_fullpath.c.path,
            ])
        .select_from(
            model.paths_fullpath
            .join(
                model.paths,
                model.paths.c.id == model.paths_fullpath.c.path_id,
                isouter=True,
                )
            )
        .where(
            model.paths.c.parent_id == None
            )
        )
    return q

def to_ncdu(findq, connection):
    """
    Format the output of 'find' so it can be read by ``ncdu -e``
    """

    findq = findq.with_only_columns([model.paths.c.id])

    paths_parents = sa.alias(model.paths_parents)

    # Get the metadata needed by ncdu
    # Found results, plus all of their parent paths
    q = (sa.select([
            model.paths.c.id,
            model.paths.c.size.label('asize'),
            model.paths.c.inode.label('ino'),
            model.basenames.c.name,
            model.paths.c.mode,
            model.paths.c.uid,
            model.paths.c.gid,
            sa.cast(model.paths.c.mtime, sa.Integer).label('mtime'),
            ])
            .select_from(model.paths
                .join(model.paths_parents,
                    model.paths_parents.c.parent_id == model.paths.c.id)
                .join(model.basenames,
                    model.paths.c.basename_id == model.basenames.c.id)
                )
            .where(model.paths_parents.c.path_id.in_(findq))
            .distinct()
        )

    tree = {None: [{"name": "."}]}
    for r in connection.execute(q):
        d = dict(r)
        d['dsize'] = d['asize']
        i = d.pop('id')
        if stat.S_ISDIR(d['mode']):
            tree[i] = [d]
        else:
            tree[i] = d

    # Get the tree edges
    q = (sa.select([
            paths_parents.c.parent_id.label('id'),
            model.paths.c.parent_id,
            ])
            .select_from(paths_parents
                .join(model.paths,
                    paths_parents.c.parent_id == model.paths.c.id,
                    isouter=True,
                ))
            .where(paths_parents.c.path_id.in_(findq))
            .distinct()
        )

    # Construct the tree relationships
    for r in connection.execute(q):
        tree[r.parent_id].append(tree[r.id])

    # Return the data ready to be converted to json
    return [1,1,{"progname": "dusql","progver": __version__,
        "timestamp": datetime.utcnow().timestamp()},tree[None]]


def find(path, connection, older_than=None, user=None, group=None, exclude=None, size=None):

    j = (model.paths_fullpath
        .join(model.paths, model.paths.c.id == model.paths_fullpath.c.path_id))

    q = (sa.sql.select([
            model.paths_fullpath.c.path,
        ])
        .select_from(j)
        )

    if path is not None:
        path_id = get_path_id(path, connection)

        j = j.join(model.paths_parents, model.paths.c.id == model.paths_parents.c.path_id)
        q = q.select_from(j).where(model.paths_parents.c.parent_id == path_id)

    if older_than is not None:
        ts = (pandas.Timestamp.now(tz='UTC') - older_than)
        ts = ts.timestamp()
        q = q.where(model.paths.c.mtime < ts)

    if user is not None:
        q = q.where(model.paths.c.uid.in_([pwd.getpwnam(u).pw_uid for u in user]))

    if group is not None:
        q = q.where(model.paths.c.gid.in_([grp.getgrnam(g).gr_gid for g in group]))

    if exclude is not None:
        excl_q = (sa.select([model.paths_parents.c.path_id])
                .select_from(model.paths_parents
                    .join(model.paths, model.paths.c.id == model.paths_parents.c.parent_id)
                    .join(model.basenames, model.basenames.c.id == model.paths.c.basename_id)
                    )
                .where(model.basenames.c.name.in_(exclude)))
        q = q.where(~model.paths.c.id.in_(excl_q))

    if size is not None:
        if size > 0:
            q = q.where(model.paths.c.size >= size)
        else:
            q = q.where(model.paths.c.size < -size)

    return q

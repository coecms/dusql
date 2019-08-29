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

import grafanadb.model as m

import sqlalchemy as sa
from collections.abc import Iterable
import os


def recursive_search(root_inodes, gid, not_gid, uid, not_uid, mtime, size):
    """
    Perform a recursive search of all files under `root_inodes` with the given constraints
    """
    roots = sa.union_all(
        *[
            sa.select(
                [sa.literal(r[0]).label("device"), sa.literal(r[1]).label("inode")]
            )
            for r in root_inodes
        ]
    ).alias("roots")

    inode = m.Inode.__table__.alias("inode")
    cte = (
        sa.select(
            [
                *inode.c,
                sa.func.dusql_path_func(
                    inode.c.parent_inode, inode.c.device, inode.c.basename
                ).label("path"),
            ]
        ).select_from(
            inode.join(
                roots,
                sa.and_(
                    inode.c.inode == roots.c.inode, inode.c.device == roots.c.device
                ),
            )
        )
    ).cte(recursive=True)

    parent = cte.alias("parent")

    child_paths = cte.union_all(
        sa.select([*inode.c, (parent.c.path + "/" + inode.c.basename).label("path")])
        .where(inode.c.parent_inode == parent.c.inode)
        .where(inode.c.device == parent.c.device)
    ).alias("find")

    q = sa.select(child_paths.c)

    if gid is not None:
        q = q.where(child_paths.c.gid == gid)
    if not_gid is not None:
        q = q.where(child_paths.c.gid != not_gid)
    if uid is not None:
        q = q.where(child_paths.c.uid == uid)
    if not_uid is not None:
        q = q.where(child_paths.c.uid != not_uid)
    if mtime is not None:
        if mtime < 0:
            q = q.where(child_paths.c.mtime <= -mtime)
        else:
            q = q.where(child_paths.c.mtime >= mtime)
    if size is not None:
        if size < 0:
            q = q.where(child_paths.c.size <= -size)
        else:
            q = q.where(child_paths.c.size >= size)

    return q


def du_impl(*args, **kwargs):
    """
    Returns the total size in bytes and inodes of paths matching the find condition
    """
    q = recursive_search(*args, **kwargs).alias("find")
    q = sa.select(
        [sa.func.sum(q.c.size).label("size"), sa.func.count().label("inodes")]
    )
    return q


def find_impl(*args, **kwargs):
    """
    Returns all paths matching the find conditions
    """
    q = recursive_search(*args, **kwargs).alias("find")
    q = sa.select([q.c.path])
    return q


def time_delta_arg(arg):
    """
    Conver a command line time argument to a posix timestamp

    arg is a string convertable by pandas into either a datetime or time delta.

    If a delta the timestamp returned is that delta before now
    """
    import pandas

    try:
        time = pandas.to_datetime(arg)
    except ValueError:
        delta = pandas.Timedelta(arg)
        time = pandas.Timestamp.utcnow() - delta
    return time.timestamp()


def size_arg(arg):
    """
    Convert a command line size argument to bytes
    """
    arg = arg.lower()

    scale = 1
    if arg.endswith("ib"):
        arg = arg[:-2]
    elif arg.endswith("b"):
        arg = arg[:-1]

    if arg.endswith("t"):
        scale = 1024 ** 4
        arg = arg[:-1]
    elif arg.endswith("g"):
        scale = 1024 ** 3
        arg = arg[:-1]
    elif arg.endswith("m"):
        scale = 1024 ** 2
        arg = arg[:-1]
    elif arg.endswith("k"):
        scale = 1024 ** 1
        arg = arg[:-1]

    value = float(arg)
    return value * scale


def find_parse(root_paths, group=None, user=None, mtime=None, size=None):
    """
    Convert the values given on the command line to the JSON needed for a
    request on the server
    """
    import grp
    import pwd

    # Convert single string/Path into an iterable
    if isinstance(root_paths, str) or not isinstance(root_paths, Iterable):
        root_paths = [root_paths]

    roots = [(s.st_dev, s.st_ino) for s in [os.stat(p) for p in root_paths]]

    response = {
        "root_inodes": roots,
        "gid": None,
        "not_gid": None,
        "uid": None,
        "not_uid": None,
        "mtime": None,
        "size": None,
    }

    if group is not None:
        if group.startswith("!") or group.startswith("-"):
            response["not_gid"] = grp.getgrnam(group[1:]).gr_gid
        else:
            response["gid"] = grp.getgrnam(group).gr_gid
    if user is not None:
        if user.startswith("!") or user.startswith("-"):
            response["not_uid"] = pwd.getpwnam(user[1:]).pw_uid
        else:
            response["uid"] = pwd.getpwnam(user).pw_uid
    if mtime is not None:
        if mtime.startswith("-"):
            response["mtime"] = -time_delta_arg(mtime[1:])
        else:
            response["mtime"] = time_delta_arg(mtime)
    if size is not None:
        response["size"] = size_arg(size)

    with open("/g/data/hh5/tmp/dusql/client_key") as f:
        response["api_key"] = f.read().strip()

    return response

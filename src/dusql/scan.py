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
from .upsert_ext import Insert
import tqdm
import os
import sqlalchemy as sa

def _walk_generator(path, parent_inode=None, progress=None):
    """
    Descend a directory, constructing a list of metadata for each file found
    """
    # Find the parent inode if not supplied
    if parent_inode is None:
        parent_inode = os.stat(path).st_ino

    for inode in os.scandir(path):
        # Loop over each file in the directory, adding it to the results list
        stat = inode.stat(follow_symlinks=False)

        yield {'name': inode.name, 'inode': stat.st_ino, 'size': stat.st_size,
                'mtime': stat.st_mtime, 'parent_inode': parent_inode, 'uid':
                stat.st_uid, 'gid': stat.st_gid}

        # Recurse into directories
        if inode.is_dir(follow_symlinks=False):
            try:
                yield from _walk_generator(inode.path, parent_inode=stat.st_ino, progress=progress)
            except FileNotFoundError:
                pass

    # Update progress bar
    if progress is not None:
        progress.update(1)


def scan(path, connection):
    """
    Recursively scan all paths under ``path``, adding their metadata to the
    database
    """

    with tqdm.tqdm(desc="Directories Scanned") as pbar:
        records = list(_walk_generator(path, progress=pbar))
        stmt = Insert(model.paths).values(records).on_conflict_do_nothing(index_elements=[model.paths.c.inode, model.paths.c.parent_inode])

        connection.execute(stmt)

def autoscan(path, connection):
    """
    Runs a scan iff path is not in the database
    """
    if path is not None:
        path_inode = os.stat(path).st_ino
        q = sa.sql.select([model.paths.c.id]).where(model.paths.c.inode == path_inode)
        r = connection.execute(q).scalar()
        if r is None:
            scan(path, connection)

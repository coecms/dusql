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
import itertools
from datetime import datetime


def _single_file(path, scan_time):
    name = os.path.basename(path)
    parent_inode = 0
    stat = os.stat(path)
    return _single_file_record(name, parent_inode, stat, scan_time)


def _single_file_record(name, parent_inode, stat, scan_time):
    return {'name': name, 'parent_inode': parent_inode, 'inode': stat.st_ino,
            'size': stat.st_size, 'mtime': stat.st_mtime, 'uid': stat.st_uid,
            'gid': stat.st_gid, 'mode': stat.st_mode, 'device': stat.st_dev,
            'ctime': stat.st_ctime, 'last_seen': scan_time}


def _walk_generator(path, parent_inode=None, progress=None, scan_time=None):
    """
    Descend a directory, constructing a list of metadata for each file found
    """
    # Find the parent inode if not supplied
    if parent_inode is None:
        parent_record = _single_file(path, scan_time)
        yield parent_record
        parent_inode = parent_record['parent_inode']

    for inode in os.scandir(path):
        # Loop over each file in the directory, adding it to the results list
        stat = inode.stat(follow_symlinks=False)

        yield _single_file_record(inode.name, parent_inode, stat, scan_time)

        # Recurse into directories
        if inode.is_dir(follow_symlinks=False):
            try:
                yield from _walk_generator(inode.path, parent_inode=stat.st_ino, progress=progress)
            except FileNotFoundError:
                pass

    # Update progress bar
    if progress is not None:
        progress.update(1)


def chunk(iterable, size):
    # From https://stackoverflow.com/a/24527424
    iterator = iter(iterable)
    for first in iterator:
        yield itertools.chain([first], itertools.islice(iterator, size-1))


def scan(path, connection):
    """
    Recursively scan all paths under ``path``, adding their metadata to the
    database
    """
    scan_time = datetime.utcnow().timestamp()

    with tqdm.tqdm(desc="Directories Scanned") as pbar:
        for records in chunk(_walk_generator(path, progress=pbar, scan_time=scan_time), 10000):
            stmt = Insert(model.paths).values(list(records)).on_conflict_do_nothing(index_elements=[model.paths.c.parent_inode, model.paths.c.inode, model.paths.c.name])

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

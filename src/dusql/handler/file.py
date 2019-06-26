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

import os
import sqlalchemy as sa
from .. import model


def get_path_id(url, conn):
    """
    Get the DB entry at url
    """
    s = os.stat(url.path)

    q = (sa.select([model.paths.c.id])
         .where(model.paths.c.inode == s.st_ino)
         .where(model.paths.c.device == s.st_dev))
    return conn.execute(q).scalar()


def _single_file(path, scan_time):
    name = os.path.basename(path)
    parent_inode = os.stat(os.path.join(path, '..')).st_ino
    parent_device = os.stat(os.path.join(path, '..')).st_dev
    stat = os.stat(path)
    return _single_file_record(name, parent_inode, parent_device, stat, scan_time)


def _single_file_record(name, parent_inode, parent_device, stat, scan_time):
    return {'name': name, 'parent_inode': parent_inode,
            'parent_device': parent_device, 'inode': stat.st_ino,
            'size': stat.st_size, 'mtime': stat.st_mtime, 'uid': stat.st_uid,
            'gid': stat.st_gid, 'mode': stat.st_mode, 'device': stat.st_dev,
            'ctime': stat.st_ctime, 'last_seen': scan_time}


def _walk_generator(path, parent_inode=None, parent_device=None, scan_time=None):
    """
    Descend a directory, constructing a list of metadata for each file found
    """
    # Find the parent inode if not supplied
    if parent_inode is None:
        parent_record = _single_file(path, scan_time)
        yield parent_record
        parent_inode = parent_record['inode']
        parent_device = parent_record['device']

    for inode in os.scandir(path):
        # Loop over each file in the directory, adding it to the results list
        stat = inode.stat(follow_symlinks=False)

        yield _single_file_record(inode.name.encode('utf-8', 'replace').decode('utf-8'), parent_inode, parent_device, stat, scan_time)

        # Recurse into directories
        if inode.is_dir(follow_symlinks=False):
            try:
                yield from _walk_generator(
                    inode.path,
                    parent_inode=stat.st_ino,
                    parent_device=stat.st_dev,
                    scan_time=scan_time)
            except FileNotFoundError:
                pass
            except PermissionError:
                pass


def scanner(url, scan_time=None):
    if isinstance(url, str):
        url = urlparse(url)

    yield from _walk_generator(url.path, scan_time=scan_time)

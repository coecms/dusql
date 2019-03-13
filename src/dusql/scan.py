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
from .find import find_children
import tqdm
import itertools
from datetime import datetime
from urllib.parse import urlunparse
import sqlalchemy as sa

from .handler import get_path_id, scanner, urlparse


def chunk(iterable, size):
    # From https://stackoverflow.com/a/24527424
    iterator = iter(iterable)
    for first in iterator:
        yield itertools.chain([first], itertools.islice(iterator, size-1))


def scan(url, connection):
    """
    Recursively scan all paths under ``path``, adding their metadata to the
    database

    ``path`` may be a local filesystem path or a URL like mdss://w35/saw562
    """
    scan_time = datetime.utcnow().timestamp()

    # Get the count currently in the db
    i = get_path_id(url, connection)
    expected = None
    if i is not None:
        expected = connection.execute(
                find_children([i])
                .with_only_columns([sa.func.count()])
                ).scalar()


    with tqdm.tqdm(total=expected, desc="Directories Scanned") as pbar:
        s = scanner(url, progress=None, scan_time=scan_time)

        for records in chunk(s, 5000):
            rs = list(records)
            stmt = (
                Insert(model.paths)
                .values(rs)
                .on_conflict_do_update(
                    values={
                        'size': 'excluded.size',
                        'mtime': 'excluded.mtime',
                        'ctime': 'excluded.ctime',
                        'uid': 'excluded.uid',
                        'gid': 'excluded.gid',
                        'mode': 'excluded.mode',
                        'device': 'excluded.device',
                        'last_seen': 'excluded.last_seen',
                        'mdss_state': 'excluded.mdss_state',
                        'links': 'excluded.links',
                        },
                    index_elements = [
                        model.paths.c.inode,
                        model.paths.c.parent_inode,
                        model.paths.c.name,
                        ]
                    )
                )

            connection.execute(stmt)
            pbar.update(len(rs))

    # Set the root path to the normalised url
    i = get_path_id(url, connection)
    connection.execute(
            Insert(model.root_paths)
            .values({'path_id': i, 'path': urlunparse(urlparse(url))})
            .on_conflict_do_nothing(index_elements=[model.root_paths.c.path_id])
            )

    # Clean out deleted files
    connection.execute(
            model.paths
            .delete()
            .where(model.paths.c.last_seen < scan_time)
            .where(model.paths.c.id.in_(find_children([i])))
            )



def autoscan(url, connection):
    """
    Runs a scan iff path is not in the database
    """
    if url is not None:
        i = get_path_id(url, connection)

        if i is None:
            scan(url, connection)

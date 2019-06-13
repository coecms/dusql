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


def _ingest_postprocess(connection):
    with connection.begin():

        # Setup basenames
        connection.execute("""
            INSERT OR IGNORE INTO basenames(name)
            SELECT name FROM paths_ingest
        """)

        # Upsert the new info
        connection.execute("""
            INSERT INTO paths (
                basename_id,
                inode,
                device,
                parent_inode,
                parent_device,
                size,
                mtime,
                ctime,
                uid,
                gid,
                mode,
                links,
                last_seen
            )
            SELECT
                basenames.id,
                paths_ingest.inode,
                paths_ingest.device,
                paths_ingest.parent_inode,
                paths_ingest.parent_device,
                paths_ingest.size,
                paths_ingest.mtime,
                paths_ingest.ctime,
                paths_ingest.uid,
                paths_ingest.gid,
                paths_ingest.mode,
                paths_ingest.links,
                paths_ingest.last_seen
            FROM paths_ingest
            JOIN basenames ON paths_ingest.name == basenames.name
            WHERE true
            ON CONFLICT (
                basename_id,
                inode,
                device,
                parent_inode,
                parent_device
            )
            DO UPDATE SET
                size = excluded.size,
                mtime = excluded.mtime,
                ctime = excluded.ctime,
                uid = excluded.uid,
                gid = excluded.gid,
                mode = excluded.mode,
                links = excluded.links,
                last_seen = excluded.last_seen
        """)

        # Update parent_id
        connection.execute("""
            UPDATE paths
            SET parent_id = (
                SELECT id
                FROM paths as parent
                WHERE
                    parent.inode == paths.parent_inode AND
                    parent.device == paths.parent_device
            ) WHERE parent_id IS NULL
        """)


def scan(url, connection):
    """
    Recursively scan all paths under ``path``, adding their metadata to the
    database

    ``path`` may be a local filesystem path or a URL like mdss://w35/saw562

    Args:
        url: Filesytem path/URL to scan
        connection: Dusql database connection from :func:`~dusql.db.connect`
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

    connection.execute(model.paths_ingest.delete())

    with tqdm.tqdm(total=expected, desc="Directories Scanned") as pbar:
        s = scanner(url, scan_time=scan_time)

        for records in chunk(s, 5000):
            rs = list(records)
            stmt = Insert(model.paths_ingest).values(rs)

            connection.execute(stmt)
            pbar.update(len(rs))

    _ingest_postprocess(connection)

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

    See :func:`scan` for arguments
    """
    if url is not None:
        i = get_path_id(url, connection)

        if i is None:
            scan(url, connection)

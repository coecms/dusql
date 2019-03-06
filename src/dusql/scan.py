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
from . import mdss
from .upsert_ext import Insert
import tqdm
import os
import sqlalchemy as sa
import itertools
import pathlib
from datetime import datetime
import urllib

from .handler import get_path_id, scanner


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

    with tqdm.tqdm(desc="Directories Scanned") as pbar:
        s = scanner(parsed_url, progress=pbar, scan_time=scan_time)

        for records in chunk(s, 10000):
            stmt = Insert(model.paths).values(list(records)).on_conflict_do_nothing(index_elements=[model.paths.c.parent_inode, model.paths.c.inode, model.paths.c.name])

            connection.execute(stmt)

def autoscan(url, connection):
    """
    Runs a scan iff path is not in the database
    """
    if url is not None:
        i = get_path_id(url, connection)

        if i is None:
            scan(url, connection)

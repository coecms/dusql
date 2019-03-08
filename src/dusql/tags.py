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

from .handler import get_path_id
from .find import find_children
from . import model

import sqlalchemy as sa
import sqlalchemy.sql.functions as safunc
from datetime import datetime


def summarise_tag(conn, tag, config):
    """
    Provide a summary of a single tag
    """
    path_ids = [get_path_id(p, conn) for p in root_paths]
    q = find_children(path_ids)
    q = q.with_only_columns([
            safunc.count().label('inodes'),
            safunc.sum(model.paths.c.size).label('size'),
            safunc.min(model.paths.c.last_seen).label('last seen'),
        ])

    r = conn.execute(q).first()
    r = dict(r)

    r['tag'] = tag

    if r['last seen'] is not None:
        r['last seen'] = datetime.fromtimestamp(r['last seen'])

    return r


def summarise_tags(conn, config):
    """
    Summarise all tags
    """
    for tag, c in config.get('tags', {}):
        yield summarise_tag(conn, tag, c)

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
from . import model

import sqlalchemy as sa
import sqlalchemy.sql.functions as safunc
from datetime import datetime

def summarise_tag(conn, tag, config):
    """
    Provide a summary of a single tag
    """

    path_ids = [get_path_id(p, conn) for p in config.get('paths', [])]
    print(path_ids)

    q = (
        sa.select([
            safunc.count().label('inodes'),
            safunc.sum(model.paths.c.size).label('size'),
            safunc.min(model.paths.c.last_seen).label('last seen'),
        ])
        .select_from(
            model.paths
            .join(
                model.paths_parents,
                model.paths.c.id == model.paths_parents.c.path_id)
            )
        .where(model.paths_parents.c.parent_id.in_(path_ids))
        )

    r = conn.execute(q).first()
    r = dict(r)

    r['tag'] = tag

    if r['last seen'] is not None:
        r['last seen'] = datetime.fromtimestamp(r['last seen'])

    return r


def summarise_tags(conn, config):
    for tag, c in config.get('tags', {}):
        yield summarise_tag(conn, tag, c)

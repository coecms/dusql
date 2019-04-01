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

from sqlalchemy.sql import dml, schema
from sqlalchemy.ext import compiler


class Insert(dml.Insert):
    """
    Custom Sqlalchemy Insert class to handle SQLite upserts

    https://www.sqlite.org/lang_UPSERT.html

    Very basic re-implementation of the Sqlalchemy postgres upsert
    """

    def on_conflict_do_nothing(self, index_elements=None):
        self._post_values_clause = OnConflictDoNothing(index_elements)
        return self

    def on_conflict_do_update(self, values, index_elements=None):
        self._post_values_clause = OnConflictDoUpdate(values, index_elements)
        return self

    def on_conflict_replace(self):
        self._post_values_clause = OnConflictReplace()
        return self


class OnConflictDoNothing(schema.ClauseElement):
    def __init__(self, index_elements=None):
        self.index_elements = index_elements


@compiler.compiles(OnConflictDoNothing)
def compile_do_nothing(element, compiler, **kw):
    conflict_target = ''

    if element.index_elements is not None:
        index_columns = [compiler.process(i, **kw)
                         for i in element.index_elements]
        conflict_target = f'({",".join(index_columns)})'

    return f'ON CONFLICT {conflict_target} DO NOTHING'


class OnConflictDoUpdate(schema.ClauseElement):
    def __init__(self, values, index_elements=None):
        self.index_elements = index_elements
        self.upsert_values = values


@compiler.compiles(OnConflictDoUpdate)
def compile_do_update(element, compiler, **kw):
    conflict_target = ''

    if element.index_elements is not None:
        index_columns = [compiler.process(i, **kw)
                         for i in element.index_elements]
        conflict_target = f'({",".join(index_columns)})'

    values = []
    for k, v in element.upsert_values.items():
        values.append(f'{k} = {v}')
    set_expr = ','.join(values)

    return f'ON CONFLICT {conflict_target} DO UPDATE SET {set_expr}'


class OnConflictReplace(schema.ClauseElement):
    pass


@compiler.compiles(OnConflictReplace)
def compile_replace(element, compiler, **kw):
    return f'ON CONFLICT REPLACE'

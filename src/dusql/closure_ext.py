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

from sqlalchemy.schema import DDLElement
from sqlalchemy.sql import table, column
from sqlalchemy.ext import compiler

class CreateClosure(DDLElement):
    def __init__(self, name, tablename, idcolumn, parentcolumn):
        self.name = name
        self.tablename = tablename
        self.idcolumn = idcolumn
        self.parentcolumn = parentcolumn


@compiler.compiles(CreateClosure)
def compile(element, compiler, **kw):
    return f'CREATE VIRTUAL TABLE {element.name} USING transitive_closure(tablename={element.tablename}, idcolumn={element.idcolumn}, parentcolumn={element.parentcolumn})'


def closure_table(name, metadata, tablename, idcolumn, parentcolumn):
    t = table(name, column('id'), column('root'), column('depth'))

    CreateClosure(name, tablename, idcolumn, parentcolumn).execute_at('after-create', metadata)

    return t


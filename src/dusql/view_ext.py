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
from sqlalchemy.sql import table
from sqlalchemy.ext import compiler
from sqlalchemy import event

class CreateView(DDLElement):
    def __init__(self, name, selectable):
        self.name = name
        self.selectable = selectable


@compiler.compiles(CreateView)
def compile_create_view(element, compiler, **kw):
    selectable = compiler.sql_compiler.process(element.selectable, literal_binds=True)
    return f'CREATE VIEW {element.name} AS {selectable}'


def should_create_view(ddl, target, bind, **kw):
    row = bind.execute("SELECT name FROM sqlite_master WHERE type='view' AND name=?",
            ddl.name).scalar()
    return not bool(row)


def view(name, metadata, selectable):
    t = table(name)

    for c in selectable.c:
        c._make_proxy(t)

    event.listen(metadata,
            'after_create',
            CreateView(name, selectable)
            .execute_if(callable_=should_create_view))

    return t

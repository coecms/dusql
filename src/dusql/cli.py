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

import argparse
from . import db

class Scan:
    def init_parser(self, parser):
        parser.add_argument('path')

    def call(self, args):
        from .scan import scan
        conn = db.connect()
        scan(args.path, conn)


class Report:
    def init_parser(self, parser):
        parser.add_argument('path')

    def call(self, args):
        from .report import report
        conn = db.connect()
        report(args.path, conn)


class Find:
    def init_parser(self, parser):
        parser.add_argument('path')
        parser.add_argument('--older_than')
        parser.add_argument('--user')
        parser.add_argument('--group')

    def call(self, args):
        from .find import find
        conn = db.connect()
        find(args.path, conn, older_than=args.older_than, user=args.user, group=args.group)


commands = {
    "scan": Scan(),
    "report": Report(),
    "find": Find(),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug')
    subp = parser.add_subparsers()

    for name, c in commands.items():
        p = subp.add_parser(name)
        p.set_defaults(func = c.call)
        c.init_parser(p)

    args = parser.parse_args()

    try:
        args.func(args)
    except Exception as e:
        if not args.debug:
            print(e)
        else:
            raise

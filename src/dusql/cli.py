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
import logging
import pandas
import re
from . import db


def parse_size(size):
    """
    Parses a find-style size into bytes
    """
    multiplier = {
        'b': 512,
        'c': 1,
        'w': 2,
        'k': 1024,
        'M': 1048576,
        'G': 1073741824,
        }
    m = re.fullmatch(r'(?P<size>[+-]?\d+(\.\d*)?)(?P<unit>[cwbkMG])', size)

    if m is None:
        raise Exception(f"Could not interpret '{size}' as a size")

    return float(m.group('size')) * multiplier[m.group('unit')]


class Scan:
    """
    """
    def init_parser(self, parser):
        parser.add_argument('path')

    def call(self, args):
        from .scan import scan
        conn = db.connect()
        scan(args.path, conn)


class Report:
    """
    """
    def init_parser(self, parser):
        parser.add_argument('path')

    def call(self, args):
        from .report import report
        conn = db.connect()
        report(args.path, conn)


class Find:
    """
    Find the full path of files in the database
    """
    def init_parser(self, parser):
        parser.add_argument('path',metavar='PATH',help="Path to search under")
        parser.add_argument('--older_than','--older-than',metavar='AGE',type=pandas.to_timedelta,help="Minimum age (e.g. '1y', '30d')")
        parser.add_argument('--user',help="Match only this user id",action='append')
        parser.add_argument('--group',help="Match only this group id",action='append')
        parser.add_argument('--exclude',help="Exclude files and directories matching this name",action='append')
        parser.add_argument('--size',type=parse_size,help="Match files greater than this find-style size (e.g. '20G') (prefix with '-' for less than)")

    def call(self, args):
        from .find import find
        conn = db.connect()
        q = find(args.path, conn, older_than=args.older_than, user=args.user, group=args.group, exclude=args.exclude, size=args.size)
        results = conn.execute(q)

        for r in results:
            print(r.path)


commands = {
    "scan": Scan(),
    "report": Report(),
    "find": Find(),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true')
    subp = parser.add_subparsers()

    logging.basicConfig()

    for name, c in commands.items():
        p = subp.add_parser(name, description=c.__doc__)
        p.set_defaults(func = c.call)
        c.init_parser(p)

    args = parser.parse_args()

    if args.debug:
        logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

    try:
        args.func(args)
    except Exception as e:
        if not args.debug:
            print(e)
        else:
            raise

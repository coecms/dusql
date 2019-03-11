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
import json
import tempfile
import sys
import subprocess
from . import db
from .config import get_config
from .scan import autoscan


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

    def call(self, args, config):
        from .scan import scan
        conn = db.connect(config['database'])
        scan(args.path, conn)


class Report:
    """
    """
    def init_parser(self, parser):
        pass

    def call(self, args, config):
        from .report import report, print_report
        conn = db.connect(config['database'])
        print_report(report(conn, config))


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
        parser.add_argument('--format',choices=['list','ncdu'],help="Output listing format")

    def call(self, args, config):
        from .find import find, to_ncdu
        conn = db.connect(config['database'])

        autoscan(args.path, conn)

        q = find(args.path, conn, older_than=args.older_than, user=args.user, group=args.group, exclude=args.exclude, size=args.size)

        if args.format is None:
            # Set default output format
            if sys.stdout.isatty():
                args.format = 'ncdu'
            else:
                args.format = 'list'

        if args.format == 'list':
            for r in conn.execute(q):
                print(r.path)

        if args.format == 'ncdu':
            with tempfile.NamedTemporaryFile('w+') as jsonf:
                # Export to JSON
                json.dump(to_ncdu(q, conn), jsonf)
                jsonf.flush()

                c = ['ncdu', '-e', '-f', jsonf.name]
                subprocess.run(c)

class PrintConfig:
    """
    Print the current configuration
    """
    def init_parser(self, parser):
        pass

    def call(self, args, config):
        import yaml
        print(yaml.dump(config, default_flow_style=False))


commands = {
    "scan": Scan(),
    "report": Report(),
    "find": Find(),
    "print-config": PrintConfig(),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--config', help='Configuration file')
    subp = parser.add_subparsers()

    logging.basicConfig()

    for name, c in commands.items():
        p = subp.add_parser(name, description=c.__doc__)
        p.set_defaults(func = c.call)
        c.init_parser(p)

    args = parser.parse_args()

    if args.debug:
        logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
        logging.getLogger(__name__).setLevel(logging.INFO)

    config = get_config(args.config)

    try:
        args.func(args, config=config)
    except Exception as e:
        if not args.debug:
            print(e)
        else:
            raise

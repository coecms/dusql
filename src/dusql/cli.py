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
    def init_parser(self, subparser):
        parser = subparser.add_parser('scan')
        parser.add_argument('path')
        parser.set_defaults(func=self.call)

    def call(self, args):
        from .scan import scan
        conn = db.connect()
        scan(args.path, conn)


commands = {
    "scan": Scan(),
    }


def main():
    parser = argparse.ArgumentParser()
    subp = parser.add_subparsers()

    for name, c in commands.items():
        c.init_parser(subp)

    args = parser.parse_args()

    args.func(args)

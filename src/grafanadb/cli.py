#!/usr/bin/env python
#
# Copyright 2019 Scott Wales
#
# Author: Scott Wales <scott.wales@unimelb.edu.au>
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

from grafanadb.find import find_parse

import argparse
import textwrap
import requests


def main():
    """
    Database-driven disk usage analysis
    """
    parser = argparse.ArgumentParser(
        description=textwrap.dedent(main.__doc__),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparser = parser.add_subparsers(help="Sub-commands")
    Find.init_parser(subparser)
    Du.init_parser(subparser)
    Ncdu.init_parser(subparser)

    args = parser.parse_args()
    if args.run is not None:
        args.run(args)


class Du:
    """
    Calculate disk usage of matching files

    Note that only CLEX project storage is available for search

    'NAME' arguments can start with either '!' or '-' to find files that don't
    have that name

    'N' arguments can start with '+' to find files greater/newer than N or '-'
    to find files less/older than N
    """

    @classmethod
    def init_parser(cls, subparser):
        parser = subparser.add_parser(
            "du",
            help="Show usage",
            description=textwrap.dedent(cls.__doc__),
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        parser.add_argument("path", nargs="+", help="Root search paths")
        parser.add_argument("--group", metavar="NAME", help="File belongs to group")
        parser.add_argument("--user", metavar="NAME", help="File belongs to user")
        parser.add_argument(
            "--mtime",
            metavar="N",
            help='File modification time (can be a year: "2018", date "20170602", or timedelta before today "1y6m")',
        )
        parser.add_argument("--size", metavar="N", help='File size ("16m", "1GB")')

        parser.set_defaults(run=cls.run)

    @classmethod
    def run(cls, args):
        for root in args.path:
            f_args = find_parse(root, args.group, args.user, args.mtime, args.size)

            r = requests.get("https://accessdev-test.nci.org.au/dusql/du", json=f_args)
            r.raise_for_status()
            r = r.json()

            print(f'{pretty_size(r["size"])}, {r["inodes"]:8d} files, {root}')


class Ncdu:
    """
    Interactively view the disk usage of matching files 

    Note that only CLEX project storage is available for search

    'NAME' arguments can start with either '!' or '-' to find files that don't
    have that name

    'N' arguments can start with '+' to find files greater/newer than N or '-'
    to find files less/older than N
    """

    @classmethod
    def init_parser(cls, subparser):
        parser = subparser.add_parser(
            "ncdu",
            help="Interactive usage viewer",
            description=textwrap.dedent(cls.__doc__),
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        parser.add_argument("path", nargs=1, help="Root search path")
        parser.add_argument("--group", metavar="NAME", help="File belongs to group")
        parser.add_argument("--user", metavar="NAME", help="File belongs to user")
        parser.add_argument(
            "--mtime",
            metavar="N",
            help='File modification time (can be a year: "2018", date "20170602", or timedelta before today "1y6m")',
        )
        parser.add_argument("--size", metavar="N", help='File size ("16m", "1GB")')

        parser.set_defaults(run=cls.run)

    @classmethod
    def run(cls, args):
        import grafanadb.ncdu as ncdu
        import curses

        f_args = find_parse(args.path, args.group, args.user, args.mtime, args.size)
        curses.wrapper(ncdu.main, args.path[0], f_args)


def pretty_size(size):
    from math import floor, log

    if size == 0:
        return "  0.00B "

    scale = floor(log(size) / log(1024))

    suffix = {0: "B ", 1: "KB", 2: "MB", 3: "GB", 4: "TB"}
    return f"{size / 1024**scale:6.2f}{suffix[scale]}"


class Find:
    """
    Find files in the Dusql database

    Note that only CLEX project storage is available for search

    'NAME' arguments can start with either '!' or '-' to find files that don't
    have that name

    'N' arguments can start with '+' to find files greater/newer than N or '-'
    to find files less/older than N
    """

    @classmethod
    def init_parser(cls, subparser):
        parser = subparser.add_parser(
            "find",
            help="Find files",
            description=textwrap.dedent(cls.__doc__),
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        parser.add_argument("roots", nargs="+", help="Root search paths")
        parser.add_argument("--group", metavar="NAME", help="File belongs to group")
        parser.add_argument("--user", metavar="NAME", help="File belongs to user")
        parser.add_argument(
            "--mtime",
            metavar="N",
            help='File modification time (can be a year: "2018", date "20170602", or timedelta before today "1y6m")',
        )
        parser.add_argument("--size", metavar="N", help='File size ("16m", "1GB")')

        parser.set_defaults(run=cls.run)

    @classmethod
    def run(cls, args):
        f_args = find_parse(args.roots, args.group, args.user, args.mtime, args.size)

        r = requests.get("https://accessdev-test.nci.org.au/dusql/find", json=f_args)
        r.raise_for_status()

        for row in r.json():
            print(row)


if __name__ == "__main__":
    main()

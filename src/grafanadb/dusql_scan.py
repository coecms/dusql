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

import tqdm
import os
import csv
import time
import argparse
import sys
import types

def scan_entry(parent_device, parent_inode, scan_time, basename, stat):
    return (
        parent_inode,
        parent_device,
        stat.st_ino,
        stat.st_dev,
        stat.st_mode,
        stat.st_uid,
        stat.st_gid,
        stat.st_size,
        stat.st_mtime,
        scan_time,
        basename.decode('utf-8', 'backslashreplace'),
        )

unreadable_stat = types.SimpleNamespace(st_ino=None, st_dev=None, st_mode=None, st_uid=None, st_gid=None, st_size=None, st_mtime=None)

def scan(path, scan_time, parent_device, parent_inode):
    try:
        with os.scandir(path) as it:
            for entry in it:
                try:
                    stat = entry.stat(follow_symlinks=False)
                except PermissionError:
                    stat = unreadable_stat
                except FileNotFoundError:
                    continue

                if entry.is_dir(follow_symlinks=False):
                    yield from scan(entry.path, scan_time, stat.st_dev, stat.st_ino)

                yield scan_entry(
                        parent_device = parent_device,
                        parent_inode = parent_inode,
                        scan_time = scan_time,
                        basename = entry.name,
                        stat = stat)

    except PermissionError:
        yield scan_entry(
                parent_device = parent_device,
                parent_inode = parent_inode,
                scan_time = scan_time,
                basename = b'',
                stat = unreadable_stat)
    except FileNotFoundError:
        return

def scan_root(root_path, csvwriter):
    broot_path = root_path.encode('utf-8')

    try:
        stat = os.stat(root_path)
    except FileNotFoundError:
        return

    csvwriter.writerow(scan_entry(parent_device = None, parent_inode = None, scan_time = None, basename = broot_path, stat = stat))

    csvwriter.writerows(
            tqdm.tqdm(
                scan(
                    broot_path,
                    scan_time = time.time(),
                    parent_device = stat.st_dev,
                    parent_inode = stat.st_ino
                    ),
                desc = root_path,
                disable = None,
                ),
            )

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('path', nargs='*')
    parser.add_argument('--output','-o',type=argparse.FileType('w'), default=sys.stdout)
    args = parser.parse_args()

    cf = csv.writer(args.output)

    for root_path in args.path:
        scan_root(root_path, cf)

if __name__ == '__main__':
    main()

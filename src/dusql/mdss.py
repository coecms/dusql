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

import re
import pandas
import stat


def scan_mdss(project, path):
    """
    Scan files in the MDSS tape store
    """
    pass


def mode_to_octal(mode):
    omode = 0

    if mode[0] == '-':
        omode |= stat.S_IFREG
    if mode[0] == 'd':
        omode |= stat.S_IFDIR
    if mode[1] == 'r':
        omode |= stat.S_IRUSR
    if mode[2] == 'w':
        omode |= stat.S_IWUSR
    if mode[3] == 'x':
        omode |= stat.S_IXUSR
    if mode[4] == 'r':
        omode |= stat.S_IRGRP
    if mode[5] == 'w':
        omode |= stat.S_IWGRP
    if mode[6] == 'x':
        omode |= stat.S_IXGRP
    if mode[6] == 's':
        omode |= stat.S_ISGID
        omode |= stat.S_IXGRP
    if mode[6] == 'S':
        omode |= stat.S_ISGID
    if mode[7] == 'r':
        omode |= stat.S_IROTH
    if mode[8] == 'w':
        omode |= stat.S_IWOTH
    if mode[9] == 'x':
        omode |= stat.S_IXOTH

    return omode



def process_entry(entry):
    date = entry.pop('date')
    time = entry.pop('time')

    entry['mtime'] = pandas.to_datetime(f'{date} {time}').timestamp()
    entry['mode'] = mode_to_octal(entry['mode'])

    return entry


def parse_mdss(stream):
    """
    Parses streamed output from `mdss dmls -aniR`, adding found files to the
    database
    """
    parent_re = re.compile(r'(?P<name>saw562):')
    count_re = re.compile(r'total (?P<count>\d+)')
    entry_re = re.compile(
            r'(?P<inode>\d+)\s+(?P<mode>\S{10})\s+(?P<links>\d+)\s+'
            r'(?P<uid>\d+)\s+(?P<gid>\d+)\s+(?P<size>\d+)\s+'
            r'(?P<date>\S{10})\s+(?P<time>\S{5})\s+\((?P<mdss_state>\S{3})\)\s+'
            r'(?P<name>.*)'
            )

    state = 'new'
    parent_name = None
    parent_entry = {}
    count = 0

    for line in stream:
        line = line.strip()

        if state == 'new':
            m = parent_re.fullmatch(line)
            if m is None:
                raise Exception(f'state "{state}" line "{line}"')
            parent_name = m.group('name')
            parent_entry = {}
            state = 'count'

        elif state == 'count':
            m = count_re.fullmatch(line)
            if m is None:
                raise Exception(f'state "{state}" line "{line}"')
            count = int(m.group('count'))
            state = 'entry'

        elif state == 'entry':
            if count == 0:
                state = 'new'
                pass
            m = entry_re.fullmatch(line)
            if m is None:
                raise Exception(f'state "{state}" line "{line}"')

            entry = m.groupdict()
            count -= 1

            # Fix types etc.
            entry = process_entry(entry)

            # Self-reference - we still need the parent inode
            if entry['name'] == '.':
                entry['name'] = parent_name
                parent_entry = entry
                continue

            # Parent reference - add the parent inode and yield the directory
            if entry['name'] == '..':
                parent_entry['parent_inode'] = entry['inode']
                yield parent_entry
                continue
            
            entry['parent_inode'] = parent_entry['inode']

            # Yield the entry
            yield entry

        else:
            raise Exception


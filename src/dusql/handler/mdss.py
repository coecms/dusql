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

import logging
import re
import pandas
import stat
import subprocess
import sqlalchemy as sa
from .. import model

parent_re = re.compile(r'(.*/)?(?P<name>.*):')
count_re = re.compile(r'total (?P<count>\d+)')
entry_re = re.compile(
        r'(?P<inode>\d+)\s+(?P<mode>\S{10})\+?\s+(?P<links>\d+)\s+'
        r'(?P<uid>\d+)\s+(?P<gid>\d+)\s+(?P<size>\d+)\s+'
        r'(?P<date>\S{10})\s+(?P<time>\S{5})\s+\((?P<mdss_state>.{3})\)\s+'
        r'(?P<name>.*)'
        )

def get_path_id(url, conn):
    """
    Get the DB entry of a path on MDSS
    """
    project = url.netloc
    path = url.path

    try:
        p = subprocess.run(
                ['/opt/bin/mdss','-P',project,'dmls','-anid','--',path],
                text=True,
                capture_output=True)
    except subprocess.CalledProcessError:
        return None

    m = entry_re.fullmatch(p.stdout.strip())
    if m is None:
        return None

    q = (sa.select([model.paths.c.id])
            .where(model.paths.c.inode == int(m.group('inode')))
            .where(model.paths.c.mdss_state != None))
    return conn.execute(q).scalar()


def scanner(url, progress=None, scan_time=None):
    """
    Scan files in the MDSS tape store
    """
    if isinstance(url, str):
        url = urlparse(url)

    project = url.netloc
    path = url.path

    if project == '':
        raise Exception('No MDSS project specified')

    cmd = ['/opt/bin/mdss','-P',project,'dmls','-aniR','--',path]
    with subprocess.Popen(cmd,
            bufsize=1,
            text=True,
            stdout=subprocess.PIPE) as p:
        yield from parse_mdss(p.stdout, progress=progress, scan_time=scan_time)
    if p.returncode != 0:
        logging.getLogger(__name__).warning(f'Command "{" ".join(p.args)}" failed with code {p.returncode}')


def mode_to_octal(mode):
    """
    Convert a text mode back to octal

    The opposite of stat.filemode
    """
    omode = 0

    if mode[0] == '-':
        omode |= stat.S_IFREG
    elif mode[0] == 'd':
        omode |= stat.S_IFDIR
    elif mode[0] == 'l':
        omode |= stat.S_IFLNK

    if mode[1] == 'r':
        omode |= stat.S_IRUSR
    if mode[2] == 'w':
        omode |= stat.S_IWUSR
    if mode[3] == 'x':
        omode |= stat.S_IXUSR
    elif mode[3] == 's':
        omode |= stat.S_ISUID
        omode |= stat.S_IXUSR
    elif mode[3] == 'S':
        omode |= stat.S_ISUID

    if mode[4] == 'r':
        omode |= stat.S_IRGRP
    if mode[5] == 'w':
        omode |= stat.S_IWGRP
    if mode[6] == 'x':
        omode |= stat.S_IXGRP
    elif mode[6] == 's':
        omode |= stat.S_ISGID
        omode |= stat.S_IXGRP
    elif mode[6] == 'S':
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

    # Convert to int
    for k in ['uid','gid','size','inode']:
        entry[k] = int(entry[k])

    return entry


def parse_mdss(stream, progress=None, scan_time=None):
    """
    Parses streamed output from `mdss dmls -aniR`, adding found files to the
    database
    """

    state = 'new'
    parent_name = None
    parent_entry = {}

    for line in stream:
        line = line.strip()

        if state == 'new':
            m = parent_re.fullmatch(line)
            if m is None:
                raise Exception(f'state "{state}" line "{line}"')
            parent_name = m.group('name')
            parent_entry = {'inode': None}
            state = 'count'

        elif state == 'count':
            m = count_re.fullmatch(line)
            if m is None:
                raise Exception(f'state "{state}" line "{line}"')
            state = 'entry'

        elif state == 'entry':
            if len(line) == 0:
                state = 'new'
                continue
            m = entry_re.fullmatch(line)
            if m is None:
                raise Exception(f'state "{state}" line "{line}"')

            entry = m.groupdict()
            entry['last_seen'] = scan_time

            # Fix types etc.
            entry = process_entry(entry)

            # Self-reference - we still need the parent inode
            if entry['name'] == '.':
                entry['name'] = parent_name
                parent_entry = entry
                continue

            if entry['name'] == '..':
                # Parent reference - add the parent inode and yield the directory
                parent_entry['parent_inode'] = entry['inode']
                yield parent_entry
            else:
                # Yield the entry
                entry['parent_inode'] = parent_entry['inode']
                yield entry

            # Update progress bar
            if progress is not None:
                progress.update(1)

        else:
            raise Exception('Error parsing MDSS state')


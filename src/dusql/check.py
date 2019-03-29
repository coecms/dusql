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

from .find import find_children
from . import model

import sqlalchemy as sa
import stat
import grp
import pwd


class Check:
    """
    Base method for all checks

    The CLI searches subclasses of this class to find available checks to run

    Subclasses should have a ``name`` attribute with the name of the check used
    by the CLI
    """

    @classmethod
    def init_parser(klass, subparser):
        """
        Helper method for the CLI to set up arguments

        Subclasses can add arguments like::

            class Group(Check):
                @classmethod
                def init_parser(klass, subparser):
                    p = super().init_parser(subparser)
                    p.add_argument('--group', required=True)
                    return p
        """
        p = subparser.add_parser(klass.name)
        p.add_argument('path')
        p.set_defaults(check = klass.cli_call)
        return p

    @classmethod
    def cli_init(klass, args):
        """
        Helper method for the CLI to initialise the class from the arguments
        set up by ``init_parser()``.

        Subclasses should implement like::

            class Group(Check):
                @classmethod
                def cli_init(klass, args):
                    return klass(group=args.group)
        """
        return klass()

    @classmethod
    def cli_call(klass, root_ids, args, connection):
        """
        Helper method for the CLI to run the check

        Overriding this shouldn't be neccessary - instead implement
        ``cli_init()``.
        """
        self = klass.cli_init(args)
        return self.query(root_ids, connection)


class OwnedBy(Check):
    """
    Check all directories under root_ids are owned by a specific group
    """
    name = 'owned-by'

    def __init__(self, user=None, group=None):
        if user is None and group is None:
            raise Exception("Please supply user and/or group")

        if user is None:
            self.uid = None
        elif isinstance(user, int):
            self.uid = user
        else:
            self.uid = pwd.getpwnam(user).pw_uid

        if group is None:
            self.gid = None
        elif isinstance(group, int):
            self.gid = group
        else:
            self.gid = grp.getgrnam(group).gr_gid

    @classmethod
    def init_parser(klass, subparser):
        p = super().init_parser(subparser)
        p.add_argument('--user')
        p.add_argument('--group')
        return p

    @classmethod
    def cli_init(klass, args):
        return klass(user=args.user, group=args.group)

    def query(self, root_ids, connection=None):
        q = find_children(root_ids)
        if self.uid is not None:
            q = q.where(~(model.paths.c.uid == self.uid))

        if self.gid is not None:
            q = q.where(~(model.paths.c.gid == self.gid))

        return q



class DirectoryGroupReadable(Check):
    """
    Check all directories under root_ids are group_readable
    """
    name = 'directory-group-readable'

    def query(self, root_ids, connection=None):
        return (find_children(root_ids)
                .where(model.paths.c.mode.op('&')(stat.S_IFDIR))
                .where(~(model.paths.c.mode.op('&')(stat.S_IRGRP | stat.S_IXGRP)))
                )


class NetCDFCompression(Check):
    """
    Check NetCDF files are compressed
    """
    name = 'netcdf-compression'

    def __init__(self, min_size, filename_pattern='*.nc'):
        self.min_size = min_size
        self.filename_pattern = filename_pattern

    def query(self, root_ids, connection):
        children = find_children(root_ids)
        netcdf_files = (children
                .select_from(
                    children
                    .join(model.basenames, model.paths.c.basename_id == model.basenames.c.id)
                    )
                .where(model.paths.c.size > self.min_size)
                .where(model.paths.c.mode.op('&')(stat.S_IFREG))
                .where(model.basenames.c.name.op('GLOB')(self.filename_pattern))
                .alias('netcdf_files')
                )

        netcdf_paths = (
                sa.select([model.paths_fullpath.c.path_id, model.paths_fullpath.c.path])
                .select_from(
                    model.paths_fullpath
                    .join(netcdf_files, netcdf_files.c.id == model.paths_fullpath.c.path_id))
                )

        found_ids = []
        for r in connection.execute(netcdf_paths):
            if self.check_file(r.path):
                found_ids.append(r.path_id)

        return sa.select([model.paths.c.id]).where(model.paths.c.id.in_(found_ids))

    def check_file(self, path):
        return True

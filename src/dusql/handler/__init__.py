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


from . import file, mdss

import urllib
import pathlib
import os

#: URL schemes dusql knows how to scan
scheme_handler = {
    '': file,
    'file': file,
    'mdss': mdss,
}


def urlparse(url):
    """
    Tidy up a URL
    """

    if isinstance(url, pathlib.PurePath):
        url = urllib.parse.urlparse(str(url))
    if isinstance(url, str):
        url = urllib.parse.urlparse(url)

    if url.scheme != '':
        url = url._replace(path=os.path.relpath(url.path, '/'))

    if url.scheme in ['', 'file']:
        url = url._replace(path=os.path.abspath(url.path))

    return url


def get_path_id(url, *args, **kwargs):
    """
    Returns the id of the model.paths record in the database matching the give
    ``url`` if one exists
    """
    url = urlparse(url)
    return scheme_handler[url.scheme].get_path_id(url, *args, **kwargs)


def scanner(url, *args, **kwargs):
    """
    Returns a generator that yields model.paths records for each path found
    under ``url``, using the handler specified by the url's scheme
    """
    url = urlparse(url)
    yield from scheme_handler[url.scheme].scanner(url, *args, **kwargs)

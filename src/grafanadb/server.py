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

from flask import Flask, request, jsonify, abort
from jsonschema import validate
from grafanadb.find import find_impl, du_impl
from grafanadb.db import connect
import os
import functools

app = Flask(__name__)
app.config["DATABASE"] = "postgresql://@/grafana"
app.config["API_KEY"] = os.environ.get("API_KEY")

find_schema = {
    "type": "object",
    "properties": {
        "root_inodes": {
            "type": "array",
            "contains": {
                "type": "array",
                "items": [{"type": "number"}, {"type": "number"}],
            },
        },
        "gid": {"type": ["number", "null"]},
        "not_gid": {"type": ["number", "null"]},
        "uid": {"type": ["number", "null"]},
        "not_uid": {"type": ["number", "null"]},
        "mtime": {"type": ["number", "null"]},
        "size": {"type": ["number", "null"]},
    },
}


@app.route("/find")
def find():
    json = request.get_json()

    print(app.config["API_KEY"])
    if json is None or json.pop("api_key", None) != app.config["API_KEY"]:
        abort(401)

    try:
        validate(json, schema=find_schema)
    except:
        abort(400)

    q = find_impl(**json)
    with connect(url=app.config["DATABASE"]) as conn:
        return jsonify([row.path for row in conn.execute(q)])

@functools.lru_cache(maxsize=8192)
def cached_du(**json):
    q = du_impl(**json)
    with connect(url=app.config["DATABASE"]) as conn:
        r = conn.execute(q).fetchone()
        return {
            "size": float(r.size) if r.size is not None else 0.0,
            "inodes": r.inodes,
        }

@app.route("/du")
def du():
    json = request.get_json()

    if json is None or json.pop("api_key", None) != app.config["API_KEY"]:
        abort(401)

    try:
        validate(json, schema=find_schema)
    except:
        abort(400)

    json['root_inodes'] = set(json['root_inodes'])

    return cached_du(**json)

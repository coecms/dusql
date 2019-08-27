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

"""
Simple python2.6 compatible bulk mailer

Reads JSON from stdin like
    
    [{
        'to': 'Sombody <someone@example.com>',
        'subject': 'Test email',
        'message': 'hello world\n'
     },
     ...
    ]

and sends emails

No external libraries so it can run on Jenkins
"""

import json
from smtplib import SMTP
from email.mime.text import MIMEText as Message
import sys

messages = json.load(sys.stdin)

server = SMTP("localhost")

for m in messages:
    toaddr = m["to"]
    fromaddr = "CLEX CMS <cws_help@nci.org.au>"

    email = Message(m["message"])
    email["Subject"] = m["subject"]
    email["From"] = fromaddr
    email["Reply-To"] = fromaddr
    email["To"] = toaddr

    server.sendmail(fromaddr, [toaddr], email.as_string())

server.quit()

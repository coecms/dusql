#!/usr/bin/env python
from setuptools import setup
import versioneer

# See setup.cfg for full metadata
setup(
        version=versioneer.get_version(),
        cmdclass=versioneer.get_cmdclass(),
        )

#!/usr/bin/env python
# This file is part of the winhpc package.
# For copyright and licensing information about this package, see the
# NOTICE.txt and LICENSE.txt files in its top-level directory; they are
# available at https://github.com/vecnet/winhpc
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License (MPL), version 2.0.  If a copy of the MPL was not distributed
# with this file, You can obtain one at http://mozilla.org/MPL/2.0/.

from setuptools import setup, find_packages

setup(
    name="vecnet.winhpc",
    version="0.7.0",
    license="MPL 2.0",
    author="Alexander Vyushkov",
    author_email="alex.vyushkov@gmail.com",
    description="Python interface for Windows HPC Server REST API",
    keywords="winhpc hpc scheduler windows webapi rest api",
    url="https://github.com/vecnet/vecnet.winhpc",
    packages=find_packages(),  # https://pythonhosted.org/setuptools/setuptools.html#using-find-packages
    install_requires=['requests'],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)


#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Radicale Storage By Index
"""

from setuptools import find_packages, setup

VERSION = '1.0.0'


tests_requirements = [
    'pytest-runner', 'pytest-cov', 'pytest-flake8', 'pytest-isort',
    'pytest'
]


options = dict(
    name="radicale-storage-by-index",
    version=VERSION,
    description="A regular radicale storage with "
    "an additional sqlite index on specified fields.",
    long_description=__doc__,
    author="Florian Mounier - Kozea",
    author_email="florian.mounier@kozea.fr",
    license="BSD",
    platforms="Any",
    install_requires=['radicale'],
    packages=find_packages(),

    setup_requires=['pytest-runner'],
    tests_require=tests_requirements,
    extras_require={'test': tests_requirements},

    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3"
    ])

setup(**options)

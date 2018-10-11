#!/usr/bin/env python

from setuptools import setup, find_packages
from glob import glob

##__________________________________________________________________||
setup(
    name='sparklight',
    version='0.0.1',
    description='A simple wrapper around Apache Spark',
    author='Dominic Smith',
    author_email='domlucasmith@gmail.com',
    packages=find_packages(exclude=['docs','images','test']),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
    ],
)

##__________________________________________________________________||

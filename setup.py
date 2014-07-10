#!/usr/bin/env python

import os
import sys

#import ez_setup
#ez_setup.use_setuptools()

from setuptools import setup, find_packages

import subprocess

VERSION_FILE="VERSION"    
    

def update_version():
    if not os.path.isdir(".git"):
        print "This does not appear to be a Git repository."
        return
    try:
        p = subprocess.Popen(["git", "describe",
                              "--tags", "--always"],
                             stdout=subprocess.PIPE)
    except EnvironmentError:
        print "Warning: Unable to run git, not modifying VERSION"
        return
    stdout = p.communicate()[0]
    if p.returncode != 0:
        print "Warning: Unable to run git, not modifying VERSION"
        return
    
    ver = stdout.strip()
    fn = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'VERSION')
    f = open(fn, "w")
    f.write(ver)
    f.close()
    print "KMeans VERSION: '%s'" % ver


def get_version():
    try:
        fn = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'VERSION')
        f = open(fn)
        version = f.read().strip()
        f.close()
    except EnvironmentError:
        return "-1"
    return version    

update_version()
    
setup(name='Pilot-KMeans',
      version=get_version(),
      description='A Pilot-based KMeans Implementation',
      author='Andre Luckow, et al.',
      author_email='aluckow@cct.lsu.edu',
      url='https://github.com/drelu/Pilot-KMeans',
      classifiers = ['Development Status :: 5 - Production/Stable',                  
                    'Programming Language :: Python',
                    'Environment :: Console',                    
                    'Topic :: Utilities',
                    ],
      platforms = ('Unix', 'Linux', 'Mac OS'),
      package_dir = {'':'src'},
      packages=['kmeans', 'distributed_inmem'],
      include_package_data=True,
      # data files for easy_install
      data_files = [('', ['README.md', 'README.md']), 
                    ('', ['VERSION', 'VERSION'])],
      
      # data files for pip
      package_data = {},

      install_requires=['bigjob2', 'argparse'],
      entry_points = { }
)

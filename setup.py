"""spark-submit module installation script"""
import os
from typing import Dict

from setuptools import find_packages, setup

info_location = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'spark_submit', '__info__.py')
about: Dict[str, str] = {}

with open(info_location, 'r') as f:
    exec(f.read(), about)

with open('README.md', 'r') as f:
    readme = f.read()

setup(
     name = about['__title__'],
     version = about['__version__'],
     author = about['__author__'],
     maintainer = about['__maintainer__'],
     author_email = about['__author_email__'],
     license = about['__license__'],
     url = about['__url__'],
     description = about['__description__'],
     long_description_content_type = 'text/markdown',
     long_description = readme,
     packages = find_packages(),
     include_package_data = True,
     install_requires = ['requests'],
     classifiers = [
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: MIT License',
          'Topic :: Software Development :: Libraries',
          'Topic :: Utilities',
          'Programming Language :: Python :: 3',
          'Operating System :: OS Independent'
     ],
     zip_safe = True,
     platforms = ['any'],
     python_requires = '~=3.6',
     keywords = ['apache', 'spark', 'submit'],
 )

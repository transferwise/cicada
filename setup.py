#!/usr/bin/env python

from setuptools import setup, find_packages

with open('README.md') as f:
    long_description = f.read()

setup(name='cicada',
      version='0.2',
      description='Lightweight, agentbased, distributed scheduler',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='Wise',
      url='https://github.com/transferwise/cicada',
      classifiers=[
        'License :: OSI Approved :: GNU Affero General Public License v3',
        'Programming Language :: Python :: 3 :: Only'
      ],
      install_requires=[
        'psycopg2-binary==2.9.3',
        'pyyaml==6.0',
        'croniter==1.2.0',
        'tabulate==0.8.9',
        'slack-sdk==3.13.0',
        'backoff==1.11.1',
      ],
      extras_require={
          'dev': [
              'pytest==6.2.5',
              'pytest-cov==3.0.0',
              'pylint==2.12.2',
          ]
      },
      entry_points={
          'console_scripts': [
            'cicada=cicada.cli:main'
          ]
      },
      packages=find_packages(include=['cicada', 'cicada.*']),
      )

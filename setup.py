#!/usr/bin/env python

from setuptools import setup, find_packages

with open("README.md") as f:
    long_description = f.read()

setup(
    name="cicada",
    version="0.3.1",
    description="Lightweight, agentbased, distributed scheduler",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Wise",
    url="https://github.com/transferwise/cicada",
    classifiers=[
        "License :: OSI Approved :: GNU Affero General Public License v3",
        "Programming Language :: Python :: 3 :: Only",
    ],
    install_requires=[
        "psycopg2-binary==2.9.3",
        "pyyaml==6.0",
        "croniter==1.3.4",
        "tabulate==0.8.9",
        "slack-sdk==3.15.2",
        "backoff==1.11.1",
        "psutil==5.9.0",
    ],
    extras_require={
        "dev": [
            "pytest==7.1.1",
            "pytest-cov==3.0.0",
            "pylint==2.12.2",
            "black==22.1.0",
            "flake8==4.0.1",
            "twine==3.8.0",
            "freezegun==1.2.1",
        ]
    },
    entry_points={"console_scripts": ["cicada=cicada.cli:main"]},
    packages=find_packages(include=["cicada", "cicada.*"]),
)

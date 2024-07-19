#!/usr/bin/env python

from setuptools import setup, find_packages

with open("README.md") as f:
    long_description = f.read()

setup(
    name="cicada",
    version="0.8.3",
    description="Lightweight, agent-based, distributed scheduler",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Wise",
    url="https://github.com/transferwise/cicada",
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3 :: Only",
    ],
    install_requires=[
        "psycopg2-binary==2.9.*",
        "pyyaml==6.0.*",
        "croniter==2.0.*",
        "tabulate==0.9.*",
        "slack-sdk==3.31.*",
        "backoff==2.2.*",
    ],
    extras_require={
        "dev": [
            "pytest==8.2.*",
            "pytest-cov==5.0.*",
            "pytest-mock==3.14.*",
            "black==24.4.*",
            "flake8==7.1.*",
            "freezegun==1.5.*",
        ]
    },
    entry_points={"console_scripts": ["cicada=cicada.cli:main"]},
    packages=find_packages(include=["cicada", "cicada.*"]),
)

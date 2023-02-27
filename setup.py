#!/usr/bin/env python

from setuptools import setup, find_packages

with open("README.md") as f:
    long_description = f.read()

setup(
    name="cicada",
    version="0.5.1",
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
        "psycopg2-binary==2.9.5",
        "pyyaml==6.0",
        "croniter==1.3.8",
        "tabulate==0.9.0",
        "slack-sdk==3.19.5",
        "backoff==2.2.1",
    ],
    extras_require={
        "dev": [
            "pytest==7.2.0",
            "pytest-cov==4.0.0",
            "pytest-mock==3.10.0",
            "black==22.12.0",
            "flake8==6.0.0",
            "freezegun==1.2.2",
        ]
    },
    entry_points={"console_scripts": ["cicada=cicada.cli:main"]},
    packages=find_packages(include=["cicada", "cicada.*"]),
)

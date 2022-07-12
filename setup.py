#!/usr/bin/env python

from setuptools import setup, find_packages

with open("README.md") as f:
    long_description = f.read()

setup(
    name="cicada",
    version="0.3.3",
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
        "psycopg2-binary==2.9.3",
        "pyyaml==6.0",
        "croniter==1.3.5",
        "tabulate==0.8.9",
        "slack-sdk==3.17.2",
        "backoff==2.1.2",
        "psutil==5.9.0",
    ],
    extras_require={
        "dev": [
            "pytest==7.1.2",
            "pytest-cov==3.0.0",
            "pylint==2.14.4",
            "black==22.3.0",
            "flake8==4.0.1",
            "freezegun==1.2.1",
        ]
    },
    entry_points={"console_scripts": ["cicada=cicada.cli:main"]},
    packages=find_packages(include=["cicada", "cicada.*"]),
)

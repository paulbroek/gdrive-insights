#!/usr/bin/env python

from setuptools import find_packages, setup

requires = []
# requires = [
#     "yapic.json>=1.7.0",
#     "pandas>=1.0.3",
#     "timeago>=1.0.15",
#     "sqlalchemy>=1.4.23",
#     "PyYAML>=5.4.1",
# ]

setup(
    name="gdrive_insights",
    version="0.0.5",
    description="Google Drive insights - \
        Show your recent Google Drive file revisions in a streamlit dashboard. Which files do you change a lot?",
    url="git@github.com:paulbroek/gdrive-insights.git",
    author="Paul Broek",
    author_email="pcbroek@paulbroek.nl",
    license="unlicense",
    install_requires=requires,
    packages=find_packages(exclude=["tests"]),
    python_requires=">=3.8",
    zip_safe=False,
)

#!/usr/bin/python
from setuptools import find_packages
from setuptools import setup

setup(
    name='ds-project',
    version='0.2',
    author='NikeNano and JohanWork',
    author_email='',
    install_requires=["joblib==0.13.2", "DateTime",
                      "google-cloud-storage", "scikit-learn"],
    packages=find_packages(exclude=['data']),
    description='Dataflow sklearn streaming',
    url=''
)

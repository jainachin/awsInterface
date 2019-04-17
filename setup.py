"""
Copyright 2019 Achin Jain (achinj@seas.upenn.edu)

"""

from setuptools import find_packages, setup

install_requires = [
    'elasticsearch',
    'boto3',
    'botocore',
    'watchdog',
    'retrying',
    'pandas',
    'requests',
    ]

setup(
    name='awsInterface',  
    author="Achin Jain",
    author_email="achinj@seas.upenn.edu",
    version = "1.0",
    packages=find_packages(),
    include_package_data=True,
    install_requires=install_requires,
)
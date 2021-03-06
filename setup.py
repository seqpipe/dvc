import platform
from setuptools import setup, find_packages
from distutils.errors import DistutilsPlatformError
from dvc import VERSION


install_requires = [
    "boto3==1.7.4",
    "ply==3.8", #required by google-gax
    "google-compute-engine==2.4.1", #required by boto
    "google-cloud-storage==1.1",
    "google-cloud==0.34.0",
    "google-cloud-core==0.24.1",
    "google-api-core==0.1.1",
    "google-auth==1.2.1",
    "google-cloud-error-reporting==0.24.2",
    "google-cloud-logging==1.0.0",
    "configparser==3.5.0",
    "zc.lockfile>=1.2.1",
    "future==0.16.0",
    "colorama>=0.3.9",
    "configobj>=5.0.6",
    "networkx>=1.11",
    "pyyaml>=3.12",
    "gitpython>=2.1.8",
    "ntfsutils>=0.1.4",
    "setuptools>=34.0.0",
    "nanotime>=0.5.2",
    "pyasn1>=0.4.1",
    "schema>=0.6.7",
    "paramiko>=2.4.1",
    "binaryornot==0.4.4",
    "jsonpath-rw==1.4.0",
]

setup(
    name='dvc',
    version=VERSION,
    description='Git for data scientists - manage your code and data together',
    long_description=open('README.rst', 'r').read(),
    author='Dmitry Petrov',
    author_email='dmitry@dataversioncontrol.com',
    download_url='https://github.com/dataversioncontrol/dvc',
    license='Apache License 2.0',
    install_requires=install_requires,
    keywords='data science, data version control, machine learning',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
    ],
    packages=find_packages(exclude=['bin', 'tests', 'functests']),
    include_package_data=True,
    url='http://dataversioncontrol.com',
    entry_points={
        'console_scripts': ['dvc = dvc.main:main']
    },
    zip_safe=False
)

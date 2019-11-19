# coding=utf-8

"""
A convenient way to use and explore multiple databases from Python.
"""

import os
from setuptools import setup, find_packages


def get_version():
    basedir = os.path.dirname(__file__)
    try:
        with open(os.path.join(basedir, 'givemedata/version.py')) as f:
            loc = {}
            exec(f.read(), loc)
            return loc['VERSION']
    except:
        raise RuntimeError('No version info found.')

setup(
    name='givemedata',
    version=get_version(),
    url='https://github.com/sancau/givemedata/',
    license='MIT',
    author='Alexander Tatchin',
    author_email='alexander.tatchin@gmail.com',
    description='A convenient way to use and explore multiple databases from Python.',
    long_description=__doc__,
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    zip_safe=False,
    platforms='any',
    install_requires=[
        'pandas>=0.24.2',
        'SQLAlchemy>=1.3.3',
        'pyyaml>=5.1',
        'psycopg2-binary>=2.8.4',
    ],
    entry_points={
        'console_scripts': [
        ],
    },
    classifiers=[
        #  As from http://pypi.python.org/pypi?%3Aaction=list_classifiers
        # 'Development Status :: 1 - Planning',
        # 'Development Status :: 2 - Pre-Alpha',
        'Development Status :: 3 - Alpha',
        # 'Development Status :: 4 - Beta',
        # 'Development Status :: 5 - Production/Stable',
        # 'Development Status :: 6 - Mature',
        # 'Development Status :: 7 - Inactive',
        'Intended Audience :: Developers',
        # 'Intended Audience :: End Users/Desktop',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Science/Research',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX',
        'Operating System :: MacOS',
        'Operating System :: Unix',
        # 'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Libraries :: Python Modules',
        # 'Topic :: Internet',
        # 'Topic :: Internet :: Log Analysis',
        'Topic :: Scientific/Engineering',
        # 'Topic :: System :: Systems Administration',
        # 'Topic :: System :: Monitoring',
        # 'Topic :: System :: Distributed Computing',
    ]
)

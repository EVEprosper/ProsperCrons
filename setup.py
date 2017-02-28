"""Setup for ProsperCrons"""

from os import path, listdir
from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand

HERE = path.abspath(path.dirname(__file__))

def hack_find_packages(include_str):
    """patches setuptools.find_packages issue

    setuptools.find_packages(path='') doesn't work as intended

    Returns:
        (:obj:`list` :obj:`str`) append <include_str>. onto every element of setuptools.find_pacakges() call

    """
    new_list = [include_str]
    for element in find_packages(include_str):
        new_list.append(include_str + '.' + element)

    return new_list

def include_all_subfiles(*args):
    """Slurps up all files in a directory (non recursive) for data_files section

    Note:
        Not recursive, only includes flat files

    Returns:
        (:obj:`list` :obj:`str`) list of all non-directories in a file

    """
    file_list = []
    for path_included in args:
        local_path = path.join(HERE, path_included)

        for file in listdir(local_path):
            file_abspath = path.join(local_path, file)
            if path.isdir(file_abspath):    #do not include sub folders
                continue
            file_list.append(path_included + '/' + file)

    return file_list

class PyTest(TestCommand):
    """PyTest cmdclass hook for test-at-buildtime functionality

    http://doc.pytest.org/en/latest/goodpractices.html#manual-integration

    """
    user_options = [('pytest-args=', 'a', "Arguments to pass to pytest")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = ['Tests']    #load defaults here

    def run_tests(self):
        import shlex
        #import here, cause outside the eggs aren't loaded
        import pytest
        pytest_commands = []
        try:    #read commandline
            pytest_commands = shlex.split(self.pytest_args)
        except AttributeError:  #use defaults
            pytest_commands = self.pytest_args
        errno = pytest.main(pytest_commands)
        exit(errno)

setup(
    name='ProsperWarehouse',
    version='0.0.0',
    license='MIT',
    classifiers=[
        'Programming Language :: Python :: 3.5'
    ],
    keywords='prosper eveonline api database',
    packages=hack_find_packages('prosper'),
    data_files=[
        ('Docs', include_all_subfiles('Docs')),
        ('Tests', include_all_subfiles('Tests'))
    ],
    package_data={
        'prosper':[
            'crons/cron_config.cfg'
        ]
    },
    install_requires=[
        'numpy==1.11.1',
        'pandas==0.18.1',
        'plumbum~=1.6.2',
        'ratelimiter==1.0.2.post0',
        'requests~=2.13',
        'retrying==1.3.3',
        'ujson==1.35',
        'wquantiles==0.4',
        'dataset~=0.8.0',
        'ProsperCommon~=0.3.8',
        #'ProsperWarehouse==0.0.4'   #TODO: remove?
    ],
    tests_require=[
        'pytest~=3.0'
    ],
    cmdclass={
        'test':PyTest
    }
)

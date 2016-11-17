'''CrestHistoryFetch.py: script for downloading CREST history for a region'''

import time #TODO: reduce to TIME or DATETIME?
from datetime import datetime
#import json #ujson
from os import path #TODO: path->plumbum?

import requests
import pandas
import numpy
from retrying import retry
import ujson as json
from plumbum import cli
from tinydb import TinyDB, Query

requests.models.json = json

from prosper.common.prosper_logging import create_logger
from prosper.common.prosper_config import get_config
import prosper.common.prosper_utilities as p_util
from prosper.warehouse.FetchConnection import *
from prosper.warehouse.Connection import *

HERE = path.abspath(path.dirname(__file__))
ME = __file__.replace('.py', '')
CONFIG_ABSPATH = path.join(HERE, 'cron_config.cfg')
config = get_config(CONFIG_ABSPATH)
logger = create_logger(
    'debug-CrestHistoryFetch',
    HERE,
    config,
    'DEBUG'
)

DEFAULT_REGION = config.get(ME, 'default_region')
CACHE_ABSPATH = path.join(HERE, config.get('GLOBAL', 'cache_path'))
if not path.exists(CACHE_ABSPATH):
    makedirs(CACHE_ABSPATH)

TYPEID_CACHE_FILE = path.join(CACHE_ABSPATH, config.get(ME, 'typeid_cache'))
TYPEID_DB = TinyDB(TYPEID_CACHE_FILE)
def get_valid_typeids(cache_filename, force_refresh=False):
    '''get valid typeids from tinydb local cache'''


@p_util.Timeit(logger)
def hello_world():
    print('hello world')

class CrestHistory(cli.Application):
    '''plumbum cli application: CrestHistory. '''
    verbose = cli.Flag(
        ['v', 'verbose'],
        help='enable logging to stdout'
    )

    uncache = cli.Flag(
        ['v', 'cache'],
        help='force cache refresh'
    )

    region_list = [DEFAULT_REGION]
    @cli.switch(
        ['-r', '--regions='],
        str,
        help='List of regions to run DEFAULT=' + region_list
    )
    def parse_regions(self, region_str_arg):
        '''parse ',' str into list for later'''
        tmp_list = region_str_arg.split(',')
        try:
            tmp_list = list(map(int, tmp_list))
        except Exception as error_msg:
            raise error_msg

        self.region_list = tmp_list

    threads = 1
    @cli.switch(
        ['-t', '--threads='],
        int,
        help='Number of threads to use to scrape endpoint: ' + str(threads)
    )
    def set_threads(self, thread_count):
        '''load thread count, else default'''
        self.threads = thread_count

    def main(self):
        '''script logic goes here'''
        print('Hello World')

if __name__ == '__main__':
    CrestHistory.run()


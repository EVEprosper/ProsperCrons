'''ScrapeCrestOrderbook.py: a script for hammering/saving the orderbook from EVE Online CREST API

    Shamelessly hacked from https://github.com/fuzzysteve/FuzzMarket'''

import time #TODO: reduce to TIME or DATETIME?
import datetime
#import json #ujson
from os import path

import requests
import pandas
import numpy
from plumbum import cli
import ujson as json
requests.models.json = json #https://github.com/kennethreitz/requests/issues/1595

from prosper.common.utilities import get_config, create_logger
from prosper.warehouse.FetchConnection import *
from prosper.warehouse.Connection import * #OPTIONAL: for Exception handling

HERE = path.abspath(path.dirname(__file__))
CONFIG_ABSPATH = path.join(HERE, 'cron_config.cfg')

config = get_config(CONFIG_ABSPATH)
logger = create_logger(
    'debug-CrestOrderBook',
    HERE,
    config,
    'DEBUG'
)

def RateLimited(max_per_second):
    '''decorator wrapper for handling ratelimiting'''
    min_interval = 1.0 / float(max_per_second)
    def decorate(func):
        last_time_called = [0.0]
        def RateLimitedFunction(*args,**kargs):
            elapsed = time.clock() - last_time_called[0]
            left_to_wait = min_interval - elapsed
            if left_to_wait>0:
                time.sleep(left_to_wait)
            ret = func(*args,**kargs)
            last_time_called[0] = time.clock()
            return ret
        return RateLimitedFunction
    return decorate

class CrestDriver(cli.Application):
    verbose = cli.Flag(
        ['v', 'verbose'],
        help='Show debug outputs')

    region_list = [10000002]
    bool_debug = False #set to run in debug/local mode

    @cli.switch(
        ['-r', '--regions='],
        str,
        help='Regions to run.  Default:' + str(region_list))
    def parse_regions(self, region_list_str):
        '''parses region argument to load region_list'''
        tmp_list = region_list_str.split(',')
        try:
            tmp_list = list(map(int, tmp_list))
        except Exception as error_msg:
            raise error_msg

        self.region_list = tmp_list

    @cli.switch(
        ['-d', '--debug'],
        help='enable debug mode: run without db connection, dump to file'
    )
    def enable_debug(self):
        '''see help -- run local-only'''
        self.bool_debug = True

    def main(self):
        '''meat of script.  Logic runs here.  Write like step list'''
        pass

if __name__ == '__main__':
    CrestDriver.run()

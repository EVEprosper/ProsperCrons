'''ScrapeCrestOrderbook.py: a script for hammering/saving the orderbook from EVE Online CREST API

    Shamelessly hacked from https://github.com/fuzzysteve/FuzzMarket'''

import time #TODO: reduce to TIME or DATETIME?
import datetime
#import json #ujson
from os import path #TODO: path->plumbum?
from timeit import timeit #todo: remove in production?

import requests
import pandas
import numpy
from retrying import retry
from ratelimiter import RateLimiter
from plumbum import cli
import ujson as json
requests.models.json = json #https://github.com/kennethreitz/requests/issues/1595

from prosper.common.utilities import get_config, create_logger
from prosper.warehouse.FetchConnection import *
from prosper.warehouse.Connection import * #OPTIONAL: for Exception handling

HERE = path.abspath(path.dirname(__file__))
ME = __file__.replace('.py', '')
CONFIG_ABSPATH = path.join(HERE, 'cron_config.cfg')

config = get_config(CONFIG_ABSPATH)
logger = create_logger(
    'debug-CrestOrderBook',
    HERE,
    config,
    'DEBUG'
)

## CREST GLOBALS ##
CREST_BASE_URL = config.get('GLOBAL', 'crest_base_url')
ENDPOINT_URI = config.get(ME, 'crest_endpoint')
PAGE_URI = config.get(ME, 'page_uri')
USERAGENT = config.get('GLOBAL', 'useragent')
RETRY_COUNT = int(config.get('GLOBAL', 'retry_count'))
RETRY_TIME = int(config.get('GLOBAL', 'retry_time')) * 1000 #ms

#def RateLimited(max_per_second):
#    '''decorator wrapper for handling ratelimiting'''
#    min_interval = 1.0 / float(max_per_second)
#    def decorate(func):
#        last_time_called = [0.0]
#        def RateLimitedFunction(*args,**kargs):
#            elapsed = time.clock() - last_time_called[0]
#            left_to_wait = min_interval - elapsed
#            if left_to_wait>0:
#                time.sleep(left_to_wait)
#            ret = func(*args,**kargs)
#            last_time_called[0] = time.clock()
#            return ret
#        return RateLimitedFunction
#    return decorate#

def timeit(method):
    '''stolen from: http://www.samuelbosch.com/2012/02/timing-functions-in-python.html'''
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        print('%r (%r, %r) %2.2f sec' % (method.__name__, args, kw, te-ts))
        return result

    return timed

@retry(
    wait_exponential_multiplier=1000,
    wait_exponential_max=RETRY_TIME,
    stop_max_attempt_number=RETRY_COUNT)
#@RateLimiter?
def GET_crest_url(url, debug=False, logger=None):
    '''GET methods for fetching CREST data'''
    response = None
    header = {
        'User-Agent': USERAGENT
    }
    debug_str = '-- Fetching ' + url
    if debug: print(debug_str)
    if logger: logger.debug(debug_str)

    try:
        request = requests.get(
            url,
            headers=header
        )
    except Exception as error_msg:
        error_str = '''EXCEPTION: request failed
    exception={exception}
    url={url}'''.\
            format(
                exception=str(error_msg),
                url=url
            )
        if debug: print(error_str)
        if logger: logger.error(error_str)

        raise error_msg #exception triggers @retry

    if request.status_code == requests.codes.ok:

        try:
            response = request.json()
        except Exception as error_msg:
            error_str = '''EXCEPTION: payload error
        exception={exception}
        url={url}'''.\
                format(
                    exception=str(error_msg),
                    url=url
                )
            if debug: print(error_str)
            if logger: logger.error(error_str)

            raise error_msg #exception triggers @retry
    else:
        error_str = '''EXCEPTION: bad status code
    exception={status_code}
    url={url}'''.\
            format(
                status_code=str(request.status_code),
                url=url
            )
        if debug: print(error_str)
        if logger: logger.error(error_str)

        raise Exception('BAD STATUS CODE: ' + str(requests.status_code))

    return response
@timeit
def test_crest_page_fetcher(base_url, debug=False, logger=None):
    test_class = CrestPageFetcher(base_url, debug, logger)
    return test_class
@timeit
def test_fancy_fetch(url, debug=False, logger=False):
    payload = GET_crest_url(url, debug, logger)
    return payload

@timeit
def test_direct_request(url):
    response = None
    header = {
        'User-Agent': USERAGENT
    }
    request = requests.get(
        url,
        headers=header
    )
    response = request.json()
    return response

class CrestPageFetcher(object):
    '''container for easier fetch/process of multi-page crest requests'''
    _debug = False
    _logger= None
    def __init__(self, base_url, debug=False, logger=None):
        '''initialize container'''
        self.all_data = []
        self.page_count = 0
        self.base_url = base_url
        self._debug = debug
        self._logger = logger
        try:
            self.page_count, self.total_count = self.get_pagecount(base_url)
        except Exception as error_msg:
            raise error_msg #logged at lower level

        print(self.page_count)
        print(self.total_count)
    def get_pagecount(self, base_url):
        '''hits endpoint to get pagecount.  Also loads first-page data to avoid retries'''
        try:
            payload = GET_crest_url(base_url)
        except Exception as error_msg:
            error_str = '''EXCEPTION: Unable to process get_pagecount request
        exception={exception}
        url={base_url}'''.\
                format(
                    exception=str(error_msg),
                    base_url=base_url
                )
            if self._debug: print(error_str)
            if self._logger: self._logger.error(error_str)
            raise error_msg

        try:
            page_count = payload['pageCount']
            total_count= payload['totalCount']
            self.all_data.extend(payload['items'])
        except KeyError as error_msg:
            error_str = '''EXCEPTION: could not find required metadata keys
        exception={exception}
        url={base_url}'''.\
                format(
                    exception=str(error_msg),
                    base_url=base_url
                )
            if self._debug: print(error_str)
            if self._logger: self._logger.error(error_str)
            raise error_msg

        return page_count, total_count

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
        for regionid in self.region_list:
            if self.verbose: print('FETCHING REGION: ' + str(regionid))
            crest_url = CREST_BASE_URL + ENDPOINT_URI
            crest_url = crest_url.format(
                regionid=regionid
                )
            if self.verbose: print('-- CREST_URL=' + crest_url)
            driver_obj = test_crest_page_fetcher(crest_url, self.bool_debug, logger)
            test_fancy_fetch(crest_url, self.bool_debug, logger)
            test_direct_request(crest_url)
if __name__ == '__main__':
    CrestDriver.run()

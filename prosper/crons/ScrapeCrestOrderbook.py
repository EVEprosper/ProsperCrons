'''ScrapeCrestOrderbook.py: a script for hammering/saving the orderbook from EVE Online CREST API

    Shamelessly hacked from https://github.com/fuzzysteve/FuzzMarket'''

import time #TODO: reduce to TIME or DATETIME?
import datetime
#import json #ujson
from os import path #TODO: path->plumbum?

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
SOLARSYSTEM_ENDPOINT = config.get(ME, 'solarsystem_endpoint')
PAGE_URI = config.get(ME, 'page_uri')
USERAGENT = config.get('GLOBAL', 'useragent')
RETRY_COUNT = int(config.get('GLOBAL', 'retry_count'))
RETRY_TIME = int(config.get('GLOBAL', 'retry_time')) * 1000 #ms
HUB_LIST = map(int,config.get(ME, 'hub_list').split(','))
DEBUG = False
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

        if DEBUG: print('-- %r %2.2f sec' % (method.__name__, te-ts))
        return result

    return timed

@retry(
    wait_exponential_multiplier=1000,
    wait_exponential_max=RETRY_TIME,
    stop_max_attempt_number=RETRY_COUNT)
#@RateLimiter?
def GET_crest_url(url, debug=DEBUG, logging=logger):
    '''GET methods for fetching CREST data'''
    response = None
    header = {
        'User-Agent': USERAGENT
    }
    debug_str = '-- Fetching ' + url
    if debug: print(debug_str)
    if logging: logging.debug(debug_str)

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
        if logging: logging.error(error_str)

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
            if logging: logging.error(error_str)

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
        if logging: logging.error(error_str)

        raise Exception('BAD STATUS CODE: ' + str(requests.status_code))

    return response

@timeit
def fetch_map_info(systemid, debug=DEBUG, logging=logger):
    '''ping CREST for map info.  Return regionid, neighbor_list'''
    solarsystem_url = CREST_BASE_URL + SOLARSYSTEM_ENDPOINT
    solarsystem_url = solarsystem_url.format(systemid=systemid)
    solarsystem_info = GET_crest_url(solarsystem_url, debug, logging)

    if debug: print('-- Fetching region info from CREST')
    if logging: logging.info('-- Fetching region info from CREST')
    constellation_url = solarsystem_info['constellation']['href']
    constellation_info = GET_crest_url(constellation_url, DEBUG, logger)
    region_url = constellation_info['region']['href']
    region_info = GET_crest_url(region_url, debug, logging) #TODO: split('/')
    regionid = int(region_info['id'])

    neighbor_list = []
    #for stargate in solarsystem_info['stargates']:
    #    stargate_name = stargate['name']
    #    if debug: print('--Fetching stargate info: ' + stargate_name)
    #    if logging: logging.info('-- Fetching stargate info: ' + stargate_name)
#
    #    stargate_url = stargate['href']
    #    stargate_info = GET_crest_url(stargate_url, debug, logging)
#
    #    neighbor_list.append(int(stargate_info['destination']['system']['id']))

    return regionid, neighbor_list

class CrestPageFetcher(object):
    '''container for easier fetch/process of multi-page crest requests'''
    _debug = False
    _logger= None
    def __init__(self, base_url, debug=DEBUG, logging=None):
        '''initialize container'''
        self.all_data = []
        self.page_count = 0
        self.base_url = base_url
        self._debug = debug
        self._logger = logging
        self.current_page = 1
        try:
            self.page_count, self.total_count = self.get_pagecount(base_url)
        except Exception as error_msg:
            raise error_msg #logged at lower level

        print(self.page_count)
        print(self.total_count)

    @timeit
    def get_pagecount(self, base_url):
        '''hits endpoint to get pagecount.  Also loads first-page data to avoid retries'''
        try:
            payload = GET_crest_url(base_url, self._debug, self._logger)
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
            #note: even 1-page resources should return pageCount/totalCount
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

    def fetch_pages(self):
        '''multi-threadded fetch of remaining pages'''
        fetched_data = []
        fetched_data[0] = self.all_data #page0 should already be loaded

    def fetch_endpoint(self):
        '''load self.all_data and return with all pages'''
        if self.current_page == self.page_count:
            #if we already walked all pages, just return data
            return self.all_data

        for page in self:
            pass #just run __iter__ it will write self.all_data

        return self.all_data

    def __iter__(self):
        '''iter magic to walk pages single-thread'''
        yield self.all_data #should already have page1 loaded by __init__
        page=2              #page1 fetched by get_pagecount() in __init__
        while page <= self.page_count:
            self.current_page = page
            page_url = self.base_url + PAGE_URI
            page_url = page_url.format(
                page_number=page
            )
            try:
                payload = GET_crest_url(page_url, self._debug, self._logger)
            except Exception as error_msg:
                error_str = '''EXCEPTION: Unable to process get_pagecount request
            exception={exception}
            url={base_url}'''.\
                    format(
                        exception=str(error_msg),
                        base_url=page_url
                    )
                if self._debug: print(error_str)
                if self._logger: self._logger.error(error_str)
                raise error_msg

            page += 1
            self.all_data.extend(payload['items'])
            yield payload['items']

def calc_percentile(
        x,
        percentile,
        #debug=DEBUG, logging=logger
):
    '''calculate percentiles off an array of values/counts'''
    print(x)
    exit()
    values = x[0]
    counts = x[1]
    stacked_vector = []
    index=0
    for value in values:
        stacked_value = [value] * counts[index]
        stacked_vector.extend(stacked_value)
        index += 1

    return numpy.percentile(stacked_vector, percentile)


@timeit
def pandify_data(data, debug=DEBUG, logging=logger):
    '''process data into dataframe for writing'''
    pd_data = pandas.DataFrame(data)

    if debug: print('-- removing unneeded data from frame')
    if logging: logging.info('-- removing unneeded data from frame')
    pd_data = pd_data[pd_data.duration < 365]   #clip all NPC orders
    pd_data_hub = pd_data[pd_data.stationID.isin(HUB_LIST)]
    pd_data_citadel = pd_data[pd_data.stationID > 70000000]
    pd_data = pd_data_hub.append(pd_data_citadel, ignore_index=True)    #save citadel/hub only
    pd_data['buy_sell'] = None
    pd_data.loc[pd_data.buy == False, 'buy_sell'] = 'SELL'
    pd_data.loc[pd_data.buy == True,  'buy_sell'] = 'BUY'
    pd_data['grouping'] = \
        pd_data['stationID'].map(str) + '-' + \
        pd_data['type'].map(str) + '-' + \
        pd_data['buy_sell'].map(str)
    pd_data['station_type'] = None
    pd_data.loc[pd_data.stationID > 70000000, 'station_type'] = 'citadelid'
    pd_data.loc[pd_data.stationID < 70000000, 'station_type'] = 'stationid'

    if debug: print('-- conditioning frame for export')
    if logging: logging.info('-- conditioning frame for export')
    #pd_sell = pd_data[pd_data.buy_sell == 'SELL']
    group = ['stationID', 'type', 'buy_sell']
    #pd_data['indx']= pd_data.groupby(group)['stationID', 'type'].\
    #    apply(calc_percentile, .75, axis=1)
    pd_data['min'] = pd_data.groupby(group)['price'].transform('min')
    pd_data['max'] = pd_data.groupby(group)['price'].transform('max')
    pd_data['vol_tot'] = pd_data.groupby(group)['volume'].transform('cumsum')
    print(pd_data.groupby(group)['price','volume'])
    #pd_sell['p_quartile'] = pd_sell.groupby(group)['price','volume'].\
    #    apply(lambda x: calc_percentile(x, percentile=75))

    print(pd_data.columns.values)

    if debug: pd_data.to_csv('test_data.csv')

    return pd_data

class CrestDriver(cli.Application):
    verbose = cli.Flag(
        ['v', 'verbose'],
        help='Show debug outputs')

    system_list = [30000142]

    @cli.switch(
        ['-r', '--regions='],
        str,
        help='Regions to run.  Default:' + str(system_list))
    def parse_regions(self, region_list_str):
        '''parses region argument to load system_list'''
        tmp_list = region_list_str.split(',')
        try:
            tmp_list = list(map(int, tmp_list))
        except Exception as error_msg:
            raise error_msg

        self.system_list = tmp_list

    @cli.switch(
        ['-d', '--debug'],
        help='enable debug mode: run without db connection, dump to file'
    )
    def enable_debug(self):
        '''see help -- run local-only'''
        global DEBUG
        DEBUG = True

    def main(self):
        '''meat of script.  Logic runs here.  Write like step list'''
        for systemid in self.system_list:
            if self.verbose: print('looking up CREST MAP info for system: ' + str(systemid))
            regionid, neighbor_list = fetch_map_info(systemid, DEBUG, logger)
            if self.verbose: print('FETCHING REGION: ' + str(regionid))
            crest_url = CREST_BASE_URL + ENDPOINT_URI
            crest_url = crest_url.format(
                regionid=regionid
                )
            if self.verbose: print('-- CREST_URL=' + crest_url)
            driver_obj = CrestPageFetcher(crest_url, DEBUG, logger)

            all_data = driver_obj.all_data
            pd_all_data = pandify_data(all_data, DEBUG, logger)

            #all_data = driver_obj.fetch_endpoint()
#
            #print(all_data[-5])
            #with open('test_data.json', 'w') as filehandle:
            #    json.dump(all_data, filehandle)

if __name__ == '__main__':
    CrestDriver.run()

'''ScrapeCrestOrderbook.py: a script for hammering/saving the orderbook from EVE Online CREST API

    Shamelessly hacked from https://github.com/fuzzysteve/FuzzMarket'''

import time #TODO: reduce to TIME or DATETIME?
from datetime import datetime
#import json #ujson
from os import path #TODO: path->plumbum?

import requests
import pandas
import numpy
from retrying import retry
from ratelimiter import RateLimiter
from weighted import quantile as wquantile #wquantile -- dev is bad and should feel bad
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
    config
)

## CREST GLOBALS ##
CREST_BASE_URL = config.get('GLOBAL', 'crest_base_url')
ENDPOINT_URI = config.get(ME, 'crest_endpoint')
SOLARSYSTEM_ENDPOINT = config.get(ME, 'solarsystem_endpoint')
PAGE_URI = config.get(ME, 'page_uri')
USERAGENT = config.get('GLOBAL', 'useragent')
RETRY_COUNT = int(config.get('GLOBAL', 'retry_count'))
RETRY_TIME = int(config.get('GLOBAL', 'retry_time')) * 1000 #ms
HUB_LIST = map(int, config.get(ME, 'hub_list').split(','))
OUTLIER_FACTOR = int(config.get(ME, 'outlier_factor'))
DEBUG = False


def timeit(method):
    '''stolen from: http://www.samuelbosch.com/2012/02/timing-functions-in-python.html'''
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        logger.debug('-- %r %2.2f sec' % (method.__name__, te-ts))
        return result

    return timed

@retry(
    wait_exponential_multiplier=1000,
    wait_exponential_max=RETRY_TIME,
    stop_max_attempt_number=RETRY_COUNT)
#@RateLimiter?
def GET_crest_url(url, debug=DEBUG):
    '''GET methods for fetching CREST data'''
    response = None
    header = {
        'User-Agent': USERAGENT
    }
    logger.info('-- Fetching ' + url)
    try:
        request = requests.get(
            url,
            headers=header
        )
    except Exception as error_msg:
        logger.error(
            'EXCEPTION: request failed ' + \
            '\r\texception={0} '.format(int(error_msg)) + \
            '\r\turl={0} '.format(url)
        )
        raise error_msg #exception triggers @retry

    if request.status_code == requests.codes.ok:
        try:
            response = request.json()
        except Exception as error_msg:
            logger.error(
                'EXCEPTION: payload error ' + \
                '\r\texception={0} '.format(str(error_msg)) + \
                '\r\turl={0} '.format(url)
            )
            raise error_msg #exception triggers @retry
    else:
        logger.error(
            'EXCEPTION: bad status code ' + \
            '\r\texception={0} '.format(request.status_code) + \
            '\r\turl={0} '.format(url)
        )
        raise Exception('BAD STATUS CODE: ' + str(requests.status_code))

    try:
        #add request_time on to the response object for record keeping
        tmp_date = request.headers['Date']
        fetch_time = datetime.strptime(tmp_date, '%a, %d %b %Y %H:%M:%S %Z')
        response['request_time'] = fetch_time.strftime('%Y-%m-%d %H:%M:%S')
    except Exception as error_msg:
        logger.warning(
            'WARNING: unable to get date from request object'
            '\r\texception={0}'.format(str(error_msg))
        )
        logger.debug(request.headers)

    return response

@timeit
def fetch_map_info(systemid, debug=DEBUG):
    '''ping CREST for map info.  Return regionid, neighbor_list'''
    logger.info('-- Fetching region info from CREST')
    solarsystem_url = CREST_BASE_URL + SOLARSYSTEM_ENDPOINT
    solarsystem_url = solarsystem_url.format(systemid=systemid)
    solarsystem_info = GET_crest_url(solarsystem_url, debug)

    constellation_url = solarsystem_info['constellation']['href']
    constellation_info = GET_crest_url(constellation_url, debug)
    region_url = constellation_info['region']['href']
    region_info = GET_crest_url(region_url, debug) #TODO: split('/')
    regionid = int(region_info['id'])

    #FIXME: vvv citadel location not supported by CREST (yet?)
#    neighbor_list = []
#    for stargate in solarsystem_info['stargates']:
#        stargate_name = stargate['name']
#        if debug: print('--Fetching stargate info: ' + stargate_name)
#        if logging: logging.info('-- Fetching stargate info: ' + stargate_name)
#
#        stargate_url = stargate['href']
#        stargate_info = GET_crest_url(stargate_url, debug, logging)
#
#        neighbor_list.append(int(stargate_info['destination']['system']['id']))

    return regionid

class CrestPageFetcher(object):
    '''container for easier fetch/process of multi-page crest requests'''
    _debug = False
    _logger= None
    def __init__(self, base_url, debug=DEBUG, logging=logger):
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

    @timeit
    def get_pagecount(self, base_url):
        '''hits endpoint to get pagecount.  Also loads first-page data to avoid retries'''
        try:
            payload = GET_crest_url(base_url, self._debug)
        except Exception as error_msg:
            self._logger.error(
                'EXCEPTION: Unable to process get_pagecount request ' + \
                '\r\texception={0} '.format(str(error_msg)) + \
                '\r\turl={0} '.format(base_url)
            )
            raise error_msg

        try:
            page_count = payload['pageCount']
            total_count= payload['totalCount']
            self.all_data.extend(payload['items'])
        except KeyError as error_msg:
            #note: even 1-page resources should return pageCount/totalCount
            self._logger.error(
                'EXCEPTION: could not find required metadata keys ' + \
                '\r\texception={0} '.format(str(error_msg)) + \
                '\r\turl={0}'.format(base_url)
            )
            raise error_msg

        return page_count, total_count

    def fetch_pages(self):
        '''multi-threadded fetch of remaining pages'''
        raise NotImplementedError('fetch_pages() for multithreaded page fetch.')
        fetched_data = []
        fetched_data[0] = self.all_data #page0 should already be loaded

    @timeit
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
                payload = GET_crest_url(page_url, self._debug)
            except Exception as error_msg:
                self._logger.error(
                    'EXCEPTION: Unable to process get_pagecount request ' + \
                    '\r\texception={0} '.format(str(error_msg)) + \
                    '\r\turl={0} '.format(page_url)
                )
                raise error_msg

            page += 1
            self.all_data.extend(payload['items'])
            yield payload['items']

def calc_mean(group_df):
    '''calculate the mean from weights'''
    count = group_df['volume'].sum()
    cumsum = group_df.cumsum()
    return cumsum/count

def wmed(group, percentile=0.5):
    '''weighted quantile measurement.  For DataFrame.groupby().apply()'''
    return wquantile(group['price'], group['volume'], percentile)

def cuttoff(group, outlier_factor=OUTLIER_FACTOR):
    '''calculate cutoff values for items.  For Dataframe.groupby().apply'''
    key_str = str(group.grouping)
    if 'BUY' in key_str:
        return pandas.Series(group.price_q25 / outlier_factor)
    elif 'SELL' in key_str:
        return pandas.Series(group.price_q75 * outlier_factor)
    else:
        raise KeyError('BUY or SELL not found in group.grouping=' + key_str)

@timeit
def pandify_data(data, datetime, debug=DEBUG):
    '''process data into dataframe for writing'''
    pd_data = pandas.DataFrame(data)

    logger.info('-- removing unneeded data from dataframe')
    pd_data = pd_data[pd_data.duration < 365]   #clip all NPC orders
    pd_data_hub = pd_data[pd_data.stationID.isin(HUB_LIST)]
    pd_data_citadel = pd_data[pd_data.stationID > 70000000]
    pd_data = pd_data_hub.append(pd_data_citadel, ignore_index=True)    #save citadel/hub only
    pd_data['buy_sell'] = None
    pd_data.loc[pd_data.buy == False, 'buy_sell'] = 'SELL'
    pd_data.loc[pd_data.buy == True,  'buy_sell'] = 'BUY'
    pd_data['station_type'] = None
    pd_data.loc[pd_data.stationID > 70000000, 'station_type'] = 'citadelid'
    pd_data.loc[pd_data.stationID < 70000000, 'station_type'] = 'stationid'

    ## NOTE: grouping is Prosper-specific.  Swap station_type/id for more generic grouping
    pd_data['grouping'] = \
        pd_data['type'].map(str) + '-' + \
        pd_data['station_type'] + '-' + \
        pd_data['buy_sell'].map(str)
        #pd_data['stationID'].map(str) + '-' + \

    logger.info('-- running statistics on dataframe')
    group = ['stationID', 'type', 'buy_sell']
    pd_data['min']     = pd_data.groupby('grouping')['price'].transform('min')
    pd_data['max']     = pd_data.groupby('grouping')['price'].transform('max')
    pd_data['vol_tot'] = pd_data.groupby('grouping')['volume'].transform('cumsum')

    pd_data['price_med'] = pd_data.groupby('grouping').apply(wmed, 0.5)
    pd_data['price_q25'] = pd_data.groupby('grouping').apply(wmed, 0.25)
    pd_data['price_q75'] = pd_data.groupby('grouping').apply(wmed, 0.75)
    #pd_data['price_cutoff'] = pd_data.groupby('grouping').apply(cuttoff)

    pd_return = pandas.DataFrame()
    pd_return['grouping'] = pd_data.groupby('grouping')
    pd_return['typeid']
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
        global DEBUG, logger
        DEBUG = True
        if self.verbose:
            logger = create_logger(
                'debug-CrestOrderBook',
                HERE,
                config,
                'DEBUG'
            )


    def main(self):
        '''meat of script.  Logic runs here.  Write like step list'''
        for systemid in self.system_list:
            #if self.verbose: print('looking up CREST MAP info for system: ' + str(systemid))
            logger.info('Looking up CREST MAP info for system ' + str(systemid))
            regionid = fetch_map_info(systemid, DEBUG)
            #if self.verbose: print('FETCHING REGION: ' + str(regionid))
            logger.info('Fetching orderbook for region: ' + str(systemid))
            crest_url = CREST_BASE_URL + ENDPOINT_URI
            crest_url = crest_url.format(
                regionid=regionid
            )
            driver_obj = CrestPageFetcher(crest_url, DEBUG)

            all_data = driver_obj.all_data
            all_data = driver_obj.fetch_endpoint()
            #pd_all_data = pandify_data(all_data, DEBUG, logger)

            ## vv DEBUG vv ##
            with open('test_data.json', 'w') as filehandle:
                json.dump(all_data, filehandle)
            ## ^^ DEBUG ^^ ##

if __name__ == '__main__':
    CrestDriver.run()

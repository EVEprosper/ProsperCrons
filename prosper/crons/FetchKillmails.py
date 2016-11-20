"""FetchKillmails.py

    Using zKillboard and EVE Online's ESI API, fetch killmails for a given daterange

"""

from datetime import datetime, timedelta
from os import path

import requests
import ujson
from plumbum import cli
from tinydb import TinyDB, Query
import dataset

import prosper.common.prosper_logging as p_logging
import prosper.common.prosper_config as p_config
#import prosper.esi.eve_esi as esi

HERE = path.abspath(path.dirname(__file__))
ME = __file__.replace('.py', '')
CONFIG_ABSPATH = path.join(HERE, 'cron_config.cfg')
config = p_config.ProsperConfig(CONFIG_ABSPATH)
LOGBUILDER = p_logging.ProsperLogger(
    ME,
    'logs',
    config
)
logger = LOGBUILDER.get_logger()

#TODO: sql credentials or table info
def get_killmails_from_db(
        start_datetime,
        end_datetime
):
    """check existing records for kms we don't need to pull

    Args:
        start_datetime (`obj`:`datetime`): start datetime range (not None)
        end_datetime (`obj`:`datetime`): end datetime range (default: today())

    Returns:
        (`obj`:`list` `list`:int) returns list of existing kill_id's between datetimes

    """
    raise NotImplementedError('TODO: not implemented yet')

#TODO: @timeit
#TODO: move to common
USERAGENT   = config.get('GLOBAL', 'useragent')
RETRY_COUNT = config.get('GLOBAL', 'retry_count')
RETRY_TIME  = config.get('GLOBAL', 'retry_time')
def fetch_address(url):
    """Reqeusts logic stacked into one call

    Args:
        url (str): url to be fetched

    Returns:
        (`obj`:`requests.json`): returns values from address

    """
    logger.debug('fetching: ' + url)

    response = None
    header = {
        'User-Agent': USERAGENT
    }
    try:
        request = requests.get(
            url,
            headers=header
        )
    except Exception as err_msg:
        logger.error(
            'EXCEPTION: request failed' +
            '\r\texception={0}'.format(err_msg) +
            '\r\turl={0}'.format(url)
        )
        raise err_msg

    if request.status_code == requests.codes.ok:
        try:
            response = request.json()
        except Exception as err_msg:
            logger.error(
                'EXCEPTION: unable to parse payload' +
                '\r\texception={0}'.format(err_msg) +
                '\r\turl={0}'.format(err_msg)
            )
            logger.debug(request.text)
    else:
        logger.error(
            'EXCEPTION: bad status code' +
            '\r\texception={0}'.format(request.status_code) +
            '\r\turl={0}'.format(url)
        )
        logger.debug(request.text)
        raise Exception('BAD STATUS CODE ' + str(request.status_code))

    #TODO: add request_time on return object?
    return response

ZKB_BULK_ENDPOINT = config.get(ME, 'zkb_bulk_endpoint')
def get_killmail_list_from_zkb(query_datetime, limit=0):
    """fetch list of killmail/hash pairs from bulk endpoint

    https://github.com/zKillboard/zKillboard/wiki/API-(History)

    Args:
        query_datetime (`obj`:`datetime`): date to fetch list of
        limit (int, optional): limit the returned values for debug mode

    Returns:
        (`obj`:`dict` `str`:`str`): returns collection of killID/killHash

    """
    date_str = query_datetime.strftime('%Y-%m-%d')
    logger.info(
        'fetching killmail list for date: ' + date_str
    )

    zkb_url = ZKB_BULK_ENDPOINT.format(
        datestr=query_datetime.strftime('%Y%m%d')
    )
    try:
        killmail_list = fetch_address(zkb_url)
    except Exception as err_msg:
        logger.error(
            'EXCEPTION caught: unable to fetch date {0}'.format(date_str) +
            '\r\texception:{0}'.format(err_msg)
        )
        return {}

    ## If limit, trim dict to limit length ##
    if limit and len(killmail_list) > limit:
        logger.info('-- trimming result to limit=' + str(limit))
        short_dict = {}
        for key, val in killmail_list.items():
            short_dict[key] = val
            if len(short_dict) >= limit:
                break
        killmail_list = short_dict

    return killmail_list

#FIXME: add ESI endpoint for killmails
CREST_KILLMAIL_ENDPOINT = config.get(ME, 'crest_killmail_endpoint')
def get_killmails_from_eve(
        zkb_km_list,
        existing_kms,
        threads=1,
        force_pull=False
):
    """get killmails from EVE API endpoint

    FIXME: ESI endpoint not available yet
    https://esi.tech.ccp.is/latest/#/
    Args
        zkb_km_list (`obj`:`requests.json`): dict of kill_id:kill_hash from zkb bulk endpoint
        existing_kms (`obj`:`list` int): list of existing km's in database to skip
        threads (int, optional): NOT IMPLEMENTED: multithreaded pull
        force_pull (bool, optional): even if km already in db, pull anyway

    Returns:
        (`obj`:`list` killmail info): returns all killmails parsed on request

    """
    logger.info(
        'Fetching {0} kms on endpoint with {1} threads'.format(len(zkb_km_list), threads)
    )

    #TODO: multithread
    kill_mail_list = []
    for kill_id, kill_hash in zkb_km_list:
        if kill_id in existing_kms:
            logger.debug('found {0} in existing list'.format(kill_id))
            if not force_pull:  #FIXME: this is dumb
                continue

        km_url = CREST_KILLMAIL_ENDPOINT.format(
            kill_id=kill_id,
            kill_hansh=kill_hash
        )
        try:
            kill_mail = fetch_address(km_url)
        except Exception as err_msg:
            logger.error(
                'EXCEPTION caught: skipping kill {0}:{1}'.format(kill_id, kill_hash) +
                '\r\texception:{0}'.format(err_msg)
            )
            #TODO: cache bad kills for later?
            continue

        kill_mail_list.append(kill_mail) #FIXME: not thread safe
        #TODO: log progress

    return kill_mail_list

class FetchKillmails(cli.Application):
    """Plumbum CLI application: FetchKillmails"""
    debug = cli.Flag(
        ['d', '--debug'],
        help='Debug mode, send data to local files'
    )
    no_db = cli.Flag(
        ['x', 'no_db'],
        help='Run without db connection.  Dump straight to file (nosql)'
    )

    limit = None
    @cli.switch(
        ['-l', '--limit'],
        int,
        help='limit the number of kills returned, early abort')
    def set_limit(self, limit_number):
        self.limit = limit_number

    threads = 1
    @cli.switch(
        ['-t', '--threads'],
        int,
        help='number of threads to use on crest/ESI fetch'
    )
    def set_threads(self, thread_count):
        self.threads = thread_count

    @cli.switch(
        ['-v', '--verbose'],
        help='enable verbose logging')
    def enable_verbose(self):
        """toggle verbose output"""
        global LOGBUILDER, logger
        LOGBUILDER.configure_default_logger(log_level='DEBUG')
        LOGBUILDER.configure_debug_logger()
        logger = LOGBUILDER.get_logger()

    start_date = None   #FIXME: required argument
    @cli.switch(
        ['-s', '--start'],
        str,
        help='startdate for desired scrape')
    def parse_start_date(self, start_date_str):
        """parse startdate from CLI"""
        try:
            self.start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        except ValueError as err_msg:
            raise err_msg

    end_date = datetime.today()
    @cli.switch(
        ['-e', '--end'],
        str,
        help='enddate for desired scrape: default=' + end_date.strftime('%Y-%m-%d'))
    def parse_end_date(self, end_date_str):
        """parse enddate from CLI: default = today"""
        try:
            self.end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
        except ValueError as err_msg:
            raise err_msg

    def main(self):
        logger.debug('hello world')

        logger.info('start_date=' + self.start_date.strftime('%Y-%m-%d'))
        logger.info('end_date=' + self.end_date.strftime('%Y-%m-%d'))

        ## Fill out list of existing db's between dates
        existing_kms = []
        if not self.no_db:
            existing_kms = get_killmails_from_db(self.start_date, self.end_date)

        query_date = self.start_date
        kms_count = 0
        bool_do_file = self.no_db or self.debug
        if bool_do_file:
            kms_processed = []
        while query_date < self.end_date:
            ## Get killmails from zkb bulk endpoint ##
            logger.info('Fetching kills for ' + query_date.strftime('%Y-%m-%d'))
            zkb_km_list = get_killmail_list_from_zkb(query_date, self.limit)
            kms_count = kms_count + len(zkb_km_list)
            if kms_count < self.limit:
                #decrement limit if required
                self.limit -= kms_count

            ## Get raw killmails from EVE (crest/ESI) ##
            kms_processed_day = get_killmails_from_eve(zkb_km_list, existing_kms)
            if bool_do_file:
                kms_processed.extend(kms_processed_day)

            if self.limit < 0:
                logger.info('limit reached, stopping queries')
                break
            query_date = self.query_date + timedelta(days=1)

if __name__ == '__main__':
    FetchKillmails.run()

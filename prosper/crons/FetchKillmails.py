"""FetchKillmails.py

    Using zKillboard and EVE Online's ESI API, fetch killmails for a given daterange

"""

from datetime import datetime
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

class FetchKillmails(cli.Application):
    """Plumbum CLI application: FetchKillmails"""
    @cli.switch(
        ['-v', '--verbose'],
        help='enable verbose logging')
    def enable_verbose(self):
        """toggle verbose output"""
        global LOGBUILDER, logger
        LOGBUILDER.configure_default_logger(log_level='DEBUG')
        LOGBUILDER.configure_debug_logger()
        logger = LOGBUILDER.get_logger()

    def main(self):
        logger.debug('hello world')

if __name__ == '__main__':
    FetchKillmails.run()

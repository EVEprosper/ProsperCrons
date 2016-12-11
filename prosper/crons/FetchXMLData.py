"""FetchXMLData.py

    Use EVE Online's v1 XML API to get server/map info

"""

from datetime import datetime, timedelta
from os import path

import requests
import ujson
from plumbum import cli
from tinydb import TinyDB, Query
import dataset

requests.models.json = ujson

import prosper.common as common
#import prosper.common.prosper_logging as p_logging
#import prosper.common.prosper_config as p_config
#import prosper.esi.eve_esi as esi

HERE = path.abspath(path.dirname(__file__))
ME = __file__.replace('.py', '')
CONFIG_ABSPATH = path.join(HERE, 'cron_config.cfg')
config = common.prosper_config.ProsperConfig(CONFIG_ABSPATH)
logger = common.DEFAULT_LOGGER

def build_logger(
        log_name=ME,
        config=config
):
    """build a logger for the script to use.
    --avoids issue of library/app fighting loggers

    Args:
        log_name (str, optional): name for logfile, default scriptname
        config (:obj:`configparser.ConfigParser`, optional): [Logging] option overrides

    Returs:
        (:obj:`prosper.common.ProsperLogger`): log builder for appending options onto

    Note:
        Pushes logger onto gloabl

    """
    global logger
    log_builder = common.prosper_logging.ProsperLogger(
        log_name,
        'logs',
        config_obj=config
    )
    logger = log_builder.logger
    return log_builder

class FetchXMLData(cli.Application):
    """Plumbum CLI application: FetchXMLData"""
    _log_builder = build_logger()
    debug = cli.Flag(
        ['d', '--debug'],
        help='Debug mode, send data to local files'
    )
    no_db = cli.Flag(
        ['x', 'no_db'],
        help='Run without db connection.  Dump straight to file (nosql)'
    )

    @cli.switch(
        ['-v', '--verbose'],
        help='enable verbose logging')
    def enable_verbose(self):
        """toggle verbose output"""
        global logger
        self._log_builder.configure_debug_logger()
        logger = self._log_builder.logger

    def main(self):
        logger.debug('hello world')

if __name__ == '__main__':
    FetchKillmails.run()

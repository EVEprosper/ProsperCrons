'''ScrapeCrestOrderbook.py: a script for hammering/saving the orderbook from EVE Online CREST API

    Shamelessly hacked from https://github.com/fuzzysteve/FuzzMarket'''

import time #TODO: reduce to TIME or DATETIME?
import datetime
import json

import requests
import pandas
import numpy
import plumbum

from prosper.common.utilities import get_config, create_logger
from prosper.warehouse.FetchConnection import *
from prosper.warehouse.Connection import * #OPTIONAL: for Exception handling

if __name__ == '__main__':
    pass

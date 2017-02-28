# ProsperCrons
ProsperCrons is the input toolset for the Prosper data-science pipeline.  There are many sources that need to be collected at regular intervals.  ProsperCrons are designed to be independent scripts that can be installed into a UNIX cron system and collect data sanely, and alert humans when things go wrong.

# Script Goals
ProsperCrons are designed to be solo and light-weight.  Each script should be able to run independently and in a self-contained manner.  When designing/writing a cron script please follow these guidelines:

* [Plumbum](http://plumbum.readthedocs.io/en/latest/cli.html) is our CLI framework
    * include a -v for verbose (see samples)
    * -d is encouraged to run without production databases
    * -h is done automatically
    * add useful filter/settings for automated/parallel runs
* [dataset](https://dataset.readthedocs.io/en/latest/) is our suggested database layer
	* [ProsperWarehouse](https://github.com/EVEprosper/ProsperWarehouse) may return to being used, but requiring two libraries to update is dumb
* Use the logger from [ProsperCommon](https://github.com/EVEprosper/ProsperCommon)
	* ERROR and higher alerts can go to Discord
	* Standardizes logger output for both verbose and text logs
* Don't forget tests
* [ujson](https://pypi.python.org/pypi/ujson) preferred when available

# Writing a cron script

The cookie cutter:
```python
#MyCron.py

from plumbum import cli

import prosper.common.prosper_logging as p_logging
import prosper.common.prosper_config as p_config 

HERE = path.abspath(path.dirname(__file__))
ME = __file__.replace('.py', '')
CONFIG_ABSPATH = path.join(HERE, 'cron_config.cfg')

config = p_config.ProsperConfig(CONFIG_ABSPATH)
logger = p_logging.DEFAULT_LOGGER

## helper classes go up here ##

## step functions go up here ##

class MyCron(cli.Application):
	"""Plumbum CLI application: MyCron"""
	_log_builder = p_logger.ProsperLogger(
		ME,
		'logs',
		config
	)
	debug = cli.Flag(
		['d', '--debug'],
        help='Debug mode, send data to local files'
	)

	@cli.switch(
        ['-v', '--verbose'],
        help='enable verbose logging')
    def enable_verbose(self):
        """toggle verbose output"""

        self._log_builder.configure_default_logger(
        	log_level='DEBUG'
        )
        self._log_builder.configure_debug_logger()

    ## cron-specific switches/flags go here ##
    
    def main(self):
    	global logger
    	logger = self._log_builder.logger

    	## step logic goes here ##
    	# stuff = get_data()
    	# clean_stuff = process_data_for_db()
    	# status = push_data_to_db()
    	# if not status:
    	#	logger.error('Something went wrong') #alert humans

if __name__ == '__main__':
	MyCron.run()
```


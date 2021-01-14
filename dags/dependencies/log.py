"""
Log

Explicit logging for dependencies
"""

from time import strftime,gmtime
import logging

class Logger():
    def __init__(self, name=None, **kwargs):
        if name is None:
            self.name = __name__
        else:
            self.name = name
        self._log(f'Log Start', name=name)
        self._logger = logging.getLogger(self.name)

        if 'use_default' in kwargs:
            self.use_default = kwargs['use_default']
        else:
            self.use_default = True

    def _log(self, message, name):
        timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        if name is None:
            name = self.name
        return f'{timestamp} {name} {message} \n'  ## TODO: REPLACE WITH STACKDRIVER CONNECTION OR SIMILAR

    def info(self, message, name=None):
        self._log(f'INFO: {message}', name)
        if self.use_default:
            self._logger.info(message)

    def debug(self, message, name=None):
        self._log(f'DEBUG: {message}', name)
        if self.use_default:
            self._logger.debug(message)

    def error(self, message, name=None):
        self._log(f'ERROR: {message}', name)
        if self.use_default:
            self._logger.error(message)


def get_logger(name:str=None) -> Logger:
    return Logger(name)
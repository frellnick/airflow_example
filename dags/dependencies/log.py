"""
Log

Explicit logging for dependencies
"""

from time import strftime,gmtime

class Logger():
    def __init__(self, name=None, **kwargs):
        if name is None:
            self.name = __name__
        else:
            self.name = name
        self._log(f'Log Start', name=name)

    def _log(self, message, name):
        timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        if name is None:
            name = self.name
        return f'{timestamp} {name} {message} \n'  ## TODO: REPLACE WITH STACKDRIVER CONNECTION OR SIMILAR

    def info(self, message, name=None):
        self._log(f'INFO: {message}', name)

    def debug(self, message, name=None):
        self._log(f'DEBUG: {message}', name)

    def error(self, message, name=None):
        self._log(f'ERROR: {message}', name)


def get_logger(name:str=None) -> Logger:
    return Logger(name)
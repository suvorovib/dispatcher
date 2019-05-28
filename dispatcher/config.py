__all__ = ['BaseConfig', 'GeneralConfig']

import os
from sanic_envconfig import EnvConfig
from enum import Enum

class LogFormat(Enum):
    json = 'json'
    plain = 'plain'

class BaseConfig(EnvConfig):

    DEBUG: bool = False
    LOG_FORMAT: LogFormat = LogFormat.plain
    LOG_LEVEL: str = 'INFO'

    # def __getattr__(self, attr):
    #     try:
    #         return self[attr]
    #     except KeyError as ke:
    #         raise AttributeError("Config has no '{}'".format(ke.args[0]))
    #
    # def __setattr__(self, attr, value):
    #     self[attr] = value


class GeneralConfig(BaseConfig):
    """
        Application config (docs https://github.com/jamesstidard/sanic-envconfig)
        """
    DEBUG: bool = False
    LOG_FORMAT: LogFormat = LogFormat.plain
    LOG_LEVEL: str = 'INFO'
    WORKERS: int = 1
    TARANTOOL_HOST: str = '0.0.0.0'
    TARANTOOL_USER: str = ''
    TARANTOOL_PASSWORD: str = ''
    TARANTOOL_Q_PORT: int = 3301
    QUEUE_POOL_SIZE: int = 5

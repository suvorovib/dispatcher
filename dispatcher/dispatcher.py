import logging.config
from structlog import get_logger
from typing import List, Tuple, Callable, Sequence, Type, Union, ClassVar

from enum import Enum

from .exc import *
from .config import BaseConfig, GeneralConfig
from .context import Context, context, _context_mutable
from .helpers import get_logging_config
from .handler import BaseHandle
from .hook import HookTypes, HookHandler

import asyncio
from asyncio import BaseEventLoop

from queues import Queues

log = get_logger('microbase')

class Dispatcher:

    queues: Queues
    config: BaseConfig
    _hooks: List[Tuple]
    _handlers: List

    def __init__(self):
        self._init_config()
        self._init_logging()
        self._init_handlers()
        self._init_hooks()

    def _init_config(self):
        self.config = BaseConfig()

    def _init_logging(self):
        self._logging_config = get_logging_config(self.config)
        logging.config.dictConfig(self._logging_config)

    def _init_handlers(self):
        self._handlers = []

    def _init_hooks(self):
        self._hooks = []

    def add_config(self, config_obj: ClassVar[BaseConfig]):
        self._import_config(config_obj)

    def _import_config(self, config_obj: ClassVar[BaseConfig]):
        for key in dir(config_obj):
            if key.isupper():
                self.config[key] = getattr(config_obj, key)

    def add_to_context(self, name, obj):
        _context_mutable.set(name, obj)

    def add_handlers(self, handlers: List[BaseHandle]):
        self._handlers.extend(handlers)

    def add_hook(self, name: HookTypes, handle):
        if not isinstance(name, HookTypes):
            raise DispatcherException('Hook must be one of HookTypes enum')
        hook_handler = HookHandler(self, handle)

        self._hooks.append((name, hook_handler))

    def _apply_logging(self):
        self._init_logging()

    def prepare(self):
        self._apply_logging()

        self.add_to_context('config', self.config)
        self.queues = Queues(host=self.config.TARANTOOL_HOST,
                             port=self.config.TARANTOOL_Q_PORT,
                             username=self.config.TARANTOOL_USER,
                             password=self.config.TARANTOOL_PASSWORD)

    def _before_start(self):
        for (hook_name, hook_handler) in self._hooks:
            if hook_name == HookTypes.before_server_start:
                hook_handler(context)

    def _after_stop(self):
        for (hook_name, hook_handler) in self._hooks:
            if hook_name == HookTypes.after_server_stop:
                hook_handler(context)

    def run(self):
        self.prepare()
        workers_futures = [self._worker(id) for id in range(self.config.WORKERS)]

        loop = asyncio.get_event_loop()

        self._before_start()

        loop.run_until_complete(asyncio.wait(workers_futures))

        self._after_stop()

        loop.close()

    async def _worker(self, id: int):
        if self.queues.is_connected == False:
            await self.queues.connect()

        while True:
            task = await self.queues.take_email_dispatch_task()

            if task is None:
                continue

            for handler in self._handlers:
                if handler.type == task.type:
                    log.info(f'Processing task {task.origin_task}')

                    try:
                        await handler.handle(context, task.payload)
                        await task.origin_task.ack()
                    except Exception as e:
                        log.error(f'Processing fail task {task.origin_task}:{e}')

                    break
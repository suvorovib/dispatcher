import logging.config
from structlog import get_logger
from typing import List, Tuple, Callable, Sequence, Type, Union, ClassVar, Dict

from enum import Enum

from .exc import *
from .config import BaseConfig, GeneralConfig
from .context import Context, context, _context_mutable
from .helpers import get_logging_config
from .handler import BaseHandle
from .hook import HookTypes, HookHandler
from .result import WorkResult, ResultAction

import asyncio
from asyncio import BaseEventLoop, sleep, Queue
from queues import Queues

log = get_logger('dispatcher')


class Dispatcher:

    queues: Queues
    config: BaseConfig
    _hooks: List[Tuple]
    _handlers: Dict[BaseHandle.type, BaseHandle.handle]
    _tube: str

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
        self._handlers = {}

    def _init_hooks(self):
        self._hooks = []

    def add_config(self, config_obj: ClassVar[BaseConfig]):
        self._import_config(config_obj)

    def _import_config(self, config_obj: ClassVar[BaseConfig]):
        for key in dir(config_obj):
            if key.isupper():
                self.config.__setattr__(key, getattr(config_obj, key))

    def add_to_context(self, name, obj):
        _context_mutable.set(name, obj)

    def add_handlers(self, handlers: List[BaseHandle]):
        for handler in handlers:
            self._handlers[handler.type] = handler.handle

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

    def _before_start(self, loop: BaseEventLoop):
        for (hook_name, hook_handler) in self._hooks:
            if hook_name == HookTypes.before_server_start:
                loop.run_until_complete(hook_handler(context))

    def _after_stop(self, loop: BaseEventLoop):
        for (hook_name, hook_handler) in self._hooks:
            if hook_name == HookTypes.after_server_stop:
                loop.run_until_complete(hook_handler(context))

    def run(self, tube: str):
        self._tube = tube
        self.prepare()
        loop = asyncio.get_event_loop()
        try:
            producers = [self._producer(id, self._tube) for id in range(self.config.WORKERS)]
    
            self._before_start(loop)
    
            [loop.create_task(producer) for producer in producers]
            loop.run_forever()
            
            self._after_stop(loop)
        except Exception as e:
            log.error(f'Exception in Dispatcher run reached')
        finally:
            loop.stop()

    def app_context(self) -> Context:
        return context

    #  deprecated
    async def _worker(self, id: int, tube: str):
        if self.queues.is_connected == False:
            await self.queues.connect()

        while True:
            task = await self.queues.take_task(tube)

            if task is None:
                continue

            if task.type in self._handlers.keys():
                log.info(f'Processing task {task.type} {task.origin_task}')

                try:
                    handle = self._handlers[task.type]
                    result: WorkResult = await handle(self, context, task.payload)

                    if result is None:
                        result = WorkResult()

                    if result.action == ResultAction.ack:
                        await task.origin_task.ack()
                    elif result.action == ResultAction.release:
                        await task.origin_task.release(delay=result.delay)
                    elif result.action == ResultAction.delete:
                        await task.origin_task.delete()
                except Exception as e:
                    log.error(f'Processing fail task {task.origin_task}:{e}')
                    await task.origin_task.release()

    async def _producer(self, id: int, tube: str):
        if not self.queues.is_connected:
            await self.queues.connect()

        while True:
            task = await self.queues.take_task(tube)

            if task is None:
                continue
            log.info(f'Producer [{id}] put task {task.type} {task.origin_task}')
            asyncio.ensure_future(self._consumer(task))

    async def _consumer(self, task):
        if task.type in self._handlers.keys():
            log.info(f' Consumer processing task {task.type} {task.origin_task}')

            try:
                handle = self._handlers[task.type]
                result: WorkResult = await handle(self, context, task.payload)

                if result is None:
                    result = WorkResult()

                if result.action == ResultAction.ack:
                    await task.origin_task.ack()
                elif result.action == ResultAction.release:
                    await task.origin_task.release(delay=result.delay)
                elif result.action == ResultAction.delete:
                    await task.origin_task.delete()
            except Exception as e:
                log.error(f'Processing fail task {task.origin_task}:{e}')
                await task.origin_task.delete()
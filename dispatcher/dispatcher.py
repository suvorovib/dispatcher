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
from asyncio import BaseEventLoop, sleep, Queue as AsyncioQueue
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
        # asyncio_queue = asyncio.Queue(loop=loop, maxsize=self.config.QUEUE_POOL_SIZE)
        try:
            self._before_start(loop)

            # loop.create_task(self._producer(asyncio_queue, self._tube))
            consumers = [self._consumer(self._tube, id) for id in range(self.config.WORKERS)]
            # [loop.create_task(consumer) for consumer in consumers]

            loop.run_until_complete(asyncio.wait(consumers))
            
            self._after_stop(loop)
        except Exception as e:
            log.error(f'Exception in Dispatcher run reached')
        finally:
            loop.stop()

    def app_context(self) -> Context:
        return context

    # async def _producer(self, asyncio_queue: AsyncioQueue, ):
    #     if not self.queues.is_connected:
    #         await self.queues.connect()
    #
    #     while True:
    #         if not asyncio_queue.full():
    #             task =
    #             if task is None:
    #                 continue
    #             await asyncio_queue.put(task)
    #             log.info(f'Producer put task {task.type} {task.origin_task}')
    #         else:
    #             await asyncio_queue.join()

    async def _consumer(self, tube: str, id: int):
        if self.queues.is_connected == False:
            await self.queues.connect()
        while True:
            try:
                task = await self.queues.take_task(tube)
                log.info(f'Consumer[{id}] get task {task.type} {task.origin_task}')
                await self._process_task(task)
                log.info(f'Consumer[{id}] finish task {task.type} {task.origin_task}')
            except Exception as e:
                log.error(f'Got exception in consumer[{id}] {e}')

    async def _process_task(self, task):
        if task.type in self._handlers.keys():
            log.info(f'Processing task {task.type} {task.origin_task}')
            try:
                handle = self._handlers[task.type]
                result: WorkResult = await handle(self, context, task.payload)

                if result is None:
                    result = WorkResult(delay=self.config.RELEASE_DELAY)

                if result.action == ResultAction.ack:
                    await task.origin_task.ack()
                elif result.action == ResultAction.release:
                    await task.origin_task.release(delay=result.delay)
                elif result.action == ResultAction.delete:
                    await task.origin_task.delete()
            except Exception as e:
                log.error(f'Processing fail task {task.origin_task}:{e}')
                await task.origin_task.ack()

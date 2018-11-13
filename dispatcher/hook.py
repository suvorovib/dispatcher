import inspect
from enum import Enum
from typing import Callable, Any

from .context import Context, context


class HookTypes(Enum):
    before_server_start = 'before_dispatcher_start'
    # after_server_start = 'after_dispatcher_start'
    # before_server_stop = 'before_dispatcher_stop'
    after_server_stop = 'after_dispatcher_stop'


class HookHandler(object):

    def __init__(self, dispatcher: Any, handler: Callable[[Any, Context], None]):
        super().__init__()
        self._dispatcher = dispatcher
        self._handler = handler

    async def __call__(self, context: Context):
        if inspect.iscoroutinefunction(self._handler):
            await self._handler(self._dispatcher, context)
        else:
            self._handler(self._dispatcher, context)
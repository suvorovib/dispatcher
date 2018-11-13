from .context import Context
from .result import WorkResult

class BaseHandle:
    type = None

    async def handle(self, app, context: Context, payload: dict) -> WorkResult:
        pass
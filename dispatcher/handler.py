from .context import Context

class BaseHandle:
    type = None

    async def handle(self, context: Context, payload: dict):
        pass
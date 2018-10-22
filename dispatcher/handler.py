from .context import Context

class BaseHandle:
    type = None

    async def handle(self, app, context: Context, payload: dict) -> bool:
        pass
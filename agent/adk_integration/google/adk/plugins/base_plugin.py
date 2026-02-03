class BasePlugin:
    def __init__(self, name: str):
        self.name = name

    async def before_run_callback(self, *, invocation_context):
        pass

from _bootstrap.context import ServiceContext
from _bootstrap.status import Stage, Phase


class ManagerStatus:

    def __init__(self, context: ServiceContext):
        self._context = context

    def __repr__(self) -> str:
        return f"<ManagerStatus stage={self._context._status}>"

    @property
    def preparing(self) -> bool:
        return self._context._status == (Stage.PREPARE, Phase.PENDING)

    @property
    def blocking(self) -> bool:
        return self._context._status == (Stage.ONLINE, Phase.PENDING)

    @property
    def cleaning(self) -> bool:
        return self._context._status == (Stage.CLEANUP, Phase.PENDING)

    async def wait_for_preparing(self):
        return await self._context.wait_for(Stage.PREPARE, Phase.PENDING)

    async def wait_for_blocking(self):
        return await self._context.wait_for(Stage.ONLINE, Phase.PENDING)

    async def wait_for_cleaning(self):
        return await self._context.wait_for(Stage.CLEANUP, Phase.PENDING)

    async def wait_for_finished(self):
        return await self._context.wait_for(Stage.EXIT, Phase.WAITING)

    async def wait_for_sigexit(self):
        return await self._context.wait_for_sigexit()

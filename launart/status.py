from _bootstrap.context import ServiceContext, _State


class _Waiter:
    def __init__(self, context: ServiceContext, state: _State):
        self.context = context
        self.state = state

    def __await__(self):
        if self.context.state is not self.state:
            yield self
        return

    __iter__ = __await__


class Status:
    def __init__(self, context: ServiceContext):
        self._context = context

    @property
    def exiting(self):
        return self._context.should_exit

    def __repr__(self) -> str:
        return f"<ManagerStatus stage={self._context._state}>"

    @property
    def preparing(self) -> bool:
        return self._context._state is _State.PREPARE_PRE

    @property
    def blocking(self) -> bool:
        return self._context.ready

    @property
    def cleaning(self) -> bool:
        return self._context._state is _State.CLEANUP_PRE

    async def wait_for_preparing(self):
        return await _Waiter(self._context, _State.PREPARE_PRE)

    async def wait_for_blocking(self):
        return await _Waiter(self._context, _State.READY)

    async def wait_for_cleaning(self):
        return await _Waiter(self._context, _State.CLEANUP_PRE)

    async def wait_for_sigexit(self):
        return await self._context.wait_for_sigexit()

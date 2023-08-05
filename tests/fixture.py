from __future__ import annotations

from launart import Launart, Service


class EmptyService(Service):
    id = "empty"
    triggered = False

    @property
    def stages(self):
        return set()

    @property
    def required(self):
        return set()

    async def launch(self, manager: Launart):
        assert Launart.current() is manager
        assert manager is self.manager
        from loguru import logger

        logger.success("EmptyService Triggered")
        self.triggered = True


def component_standalone(component_id: str, required_ids: list[str]) -> Service:
    class _L(Service):
        id = component_id

        @property
        def required(self):
            return set(required_ids)

        @property
        def _required_id(self):
            return set(required_ids)

        @property
        def stages(self):
            return set()

        async def launch(self, _):
            ...

    return _L()


def component(component_id: str, required_ids: list[str]) -> Service:
    class _L(Service):
        id = component_id

        @property
        def required(self):
            return set(required_ids)

        @property
        def stages(self):
            return set()

        async def launch(self, _):
            ...

    return _L()


def service(component_id: str, required_ids: list[str]) -> Service:
    class Srv(Service):
        id = component_id

        @property
        def required(self):
            return set(required_ids)

        @property
        def stages(self):
            return set()

        async def launch(self, _):
            ...


    return Srv()
